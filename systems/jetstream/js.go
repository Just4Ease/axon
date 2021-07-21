package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/nats-io/nats.go"
	"log"
	"strings"
	"sync"
	"time"
)

const Empty = ""

type natsStore struct {
	codec.Codec
	opts               options.Options
	natsClient         *nats.Conn
	jsmClient          nats.JetStreamContext
	mu                 *sync.RWMutex
	subscriptions      map[string]*subscription
	publishTopics      map[string]string
	knownSubjectsCount int
	serviceName        string
}

func (s *natsStore) newCodec(contentType string) (codec.NewCodec, error) {
	if c, ok := s.opts.Codecs[contentType]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("unsupported Content-Type: %s", contentType)
}

func (s *natsStore) Request(message *messages.Message) (*messages.Message, error) {
	nc := s.natsClient
	message.WithType(messages.RequestMessage)
	message.WithSource(s.opts.ServiceName)
	if message.SpecVersion == Empty {
		message.WithSpecVersion("default")
	}

	data, err := s.opts.Marshal(message)
	if err != nil {
		return nil, err
	}

	msg, err := nc.Request(message.Subject, data, time.Second)
	if err != nil {
		return nil, err
	}

	var mg messages.Message
	if err := s.opts.Unmarshal(msg.Data, &mg); err != nil {
		log.Print("failed to unmarshal reply event into reply struct with the following errors: ", err)
		_ = msg.Nak()
		return nil, err
	}

	_ = msg.Ack()

	return &mg, nil
}

func (s *natsStore) Reply(topic string, handler axon.ReplyHandler) error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		_, err := s.natsClient.QueueSubscribe(topic, s.opts.ServiceName, func(msg *nats.Msg) {
			var mg messages.Message
			if err := s.opts.Unmarshal(msg.Data, &mg); err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				errChan <- err
				return
			}

			responseMessage, responseError := handler(&mg)
			if responseError != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", responseError)
				responseMessage = messages.NewMessage()
				responseMessage.Error = responseError.Error()
				responseMessage.WithType(messages.ErrorMessage)
			} else {
				responseMessage.WithType(messages.ResponseMessage)
			}
			responseMessage.WithSpecVersion(mg.SpecVersion)
			responseMessage.WithSource(s.opts.ServiceName)
			responseMessage.WithSubject(topic)

			data, err := s.opts.Marshal(responseMessage)
			if err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				errChan <- err
				return
			}

			if err := msg.Respond(data); err != nil {
				log.Print("failed to reply data to the incoming request with the following error: ", err)
				errChan <- err
				return
			}
		})
		if err != nil {
			errChan <- err
		}
	}(errChan)
	return <-errChan
}

func (s *natsStore) GetServiceName() string {
	return s.opts.ServiceName
}

func Init(opts options.Options, options ...nats.Option) (axon.EventStore, error) {

	addr := strings.TrimSpace(opts.Address)
	if addr == "" {
		return nil, axon.ErrInvalidURL
	}

	name := strings.TrimSpace(opts.ServiceName)
	if name == "" {
		return nil, axon.ErrEmptyStoreName
	}
	opts.ServiceName = strings.TrimSpace(name)
	options = append(options, nats.Name(name))
	if opts.AuthenticationToken != "" {
		options = append(options, nats.Token(opts.AuthenticationToken))
	}

	nc, js, err := connect(opts.ServiceName, opts.Address, options)

	if err != nil {
		return nil, err
	}

	return &natsStore{
		opts:               opts,
		jsmClient:          js,
		natsClient:         nc,
		serviceName:        name,
		subscriptions:      make(map[string]*subscription),
		publishTopics:      make(map[string]string),
		knownSubjectsCount: 0,
		mu:                 &sync.RWMutex{},
	}, nil
}

func (s *natsStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		handler.Run()
	}

	s.registerSubjectsOnStream()

	for _, sub := range s.subscriptions {
		go sub.runSubscriptionHandler()
	}

	<-ctx.Done()
}

func (s *natsStore) registerSubjectsOnStream() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var subjects []string

	for _, v := range s.subscriptions {
		subjects = append(subjects, v.topic)
	}

	for _, topic := range s.publishTopics {
		subjects = append(subjects, topic)
	}

	subjects = append(subjects, s.opts.ServiceName)
	// Do not bother altering the stream state if the values are the same.
	if len(subjects) == s.knownSubjectsCount {
		return
	}

	s.knownSubjectsCount = len(subjects)

	if _, err := s.jsmClient.UpdateStream(&nats.StreamConfig{
		Name:     s.opts.ServiceName,
		Subjects: subjects,
		NoAck:    false,
	}); err != nil {
		fmt.Printf("error updating stream: %s \n", err)
		if err.Error() == "duplicate subjects detected" {
			streamInfo, _ := s.jsmClient.StreamInfo(s.opts.ServiceName)
			if len(streamInfo.Config.Subjects) != len(subjects) {
				_ = s.jsmClient.DeleteStream(s.opts.ServiceName)
				time.Sleep(1 * time.Second)
				streamInfo, _ = s.jsmClient.AddStream(&nats.StreamConfig{
					Name:     s.opts.ServiceName,
					Subjects: subjects,
					MaxAge:   time.Hour * 48,
					NoAck:    false,
				})
				PrettyJson(streamInfo)
			}
		}
	}
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}

func connect(sn, addr string, options []nats.Option) (*nats.Conn, nats.JetStreamContext, error) {
	nc, err := nats.Connect(addr, options...)
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, nil, err
	}

	sinfo, err := js.StreamInfo(sn)
	if err != nil {
		if err.Error() != "stream not found" {
			return nil, nil, err
		}

		if sinfo, err = js.AddStream(&nats.StreamConfig{
			Name:     sn,
			Subjects: []string{sn},
			NoAck:    false,
		}); err != nil {
			return nil, nil, err
		}
	}

	fmt.Printf("JetStream Server Info: %v \n", sinfo)
	return nc, js, nil
}
