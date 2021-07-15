package jetstream

import (
	"context"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"log"
	"strings"
	"sync"
	"time"
)

const Empty = ""

type natsStore struct {
	codec.Codec
	opts       options.Options
	natsClient *nats.Conn
	jsmClient  nats.JetStreamContext
	mu         *sync.RWMutex
	subjects   []string
}

func (s *natsStore) newCodec(contentType string) (codec.NewCodec, error) {
	if c, ok := s.opts.Codecs[contentType]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("unsupported Content-Type: %s", contentType)
}

func (s *natsStore) Publish(message *messages.Message) error {
	if message == nil {
		return errors.New("invalid message")
	}

	message.WithType(messages.EventMessage)
	data, err := s.opts.Marshal(message)
	if err != nil {
		return err
	}
	_, err = s.jsmClient.Publish(message.Subject, data)
	return err
}

func (s *natsStore) Subscribe(topic string, handler axon.SubscriptionHandler, opts ...*options.SubscriptionOptions) error {
	if err := s.registerSubjectOnStream(topic); err != nil {
		return err
	}

	lb := fmt.Sprintf("%s-%s", s.opts.ServiceName, topic)
	fmt.Printf("Load Balance Group: %s", lb)

	durableName := strings.ReplaceAll(lb, ".", "-")

	var sub *nats.Subscription
	errChan := make(chan error)
	so := options.MergeSubscriptionOptions(opts...)

	go func(so *options.SubscriptionOptions, sub *nats.Subscription, errChan chan<- error) {
		subType := so.GetSubscriptionType()
		var err error
		if subType == options.Shared {
			_, err = s.jsmClient.QueueSubscribe(topic, durableName, func(m *nats.Msg) {
				var msg messages.Message
				if err = s.opts.Unmarshal(m.Data, &msg); err != nil {
					errChan <- err
					return
				}

				event := newEvent(m, msg)
				go handler(event)
			},
				nats.Durable(durableName),
				//nats.
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.opts.ServiceName),
				//nats.AckNone(),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(5),
			)
		}

		if subType == options.KeyShared {
			_, err = s.jsmClient.Subscribe(topic, func(m *nats.Msg) {
				var msg messages.Message
				if err = s.opts.Unmarshal(m.Data, &msg); err != nil {
					errChan <- err
					return
				}

				event := newEvent(m, msg)
				go handler(event)
			},
				nats.Durable(durableName),
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.opts.ServiceName),
				//nats.AckExplicit(),
				//nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(5))

		}
	}(so, sub, errChan)

	//defer sub.Drain()
	//defer cancel()
	for {
		select {
		case <-so.GetContext().Done():
			return nil
		case err := <-errChan:
			return err
		}
	}
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

//
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

func (s *natsStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}

	<-ctx.Done()
}

func Init(opts options.Options, options ...nats.Option) (axon.EventStore, error) {

	opts.EnsureDefaultMarshaling()
	addr := strings.TrimSpace(opts.Address)
	if addr == "" {
		return nil, axon.ErrInvalidURL
	}

	if strings.TrimSpace(opts.ServiceName) == Empty {
		return nil, axon.ErrEmptyStoreName
	}

	//if opts.Marshaler == nil {
	//	opts.Marshaler = msgpack.Marshaler{}
	//}

	//if opts.AuthenticationToken != "" {
	//	clientOptions.Authentication = pulsar.NewAuthenticationToken(opts.AuthenticationToken)
	//}

	log.Printf("Started event store with service name: %s \n", opts.ServiceName)

	options = append(options, nats.Name(opts.ServiceName))

	nc, err := nats.Connect(opts.Address, options...)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	sinfo, err := js.StreamInfo(opts.ServiceName)
	if err != nil {
		if err.Error() != "stream not found" {
			return nil, err
		}

		if sinfo, err = js.AddStream(&nats.StreamConfig{
			Name: opts.ServiceName,
			//Retention: nats.InterestPolicy,
			//NoAck: true,
			NoAck: false,
		}); err != nil {
			return nil, err
		}
	}

	log.Print(sinfo.Config, " Stream Config \n")

	return &natsStore{
		jsmClient:  js,
		natsClient: nc,
		opts:       opts,
		subjects:   make([]string, 0),
		mu:         &sync.RWMutex{},
	}, nil
}

func (s *natsStore) registerSubjectOnStream(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.subjects {
		if v == subject {
			return nil
		}

		s.subjects = append(s.subjects, subject)
	}

	if len(s.subjects) == 0 {
		s.subjects = append(s.subjects, subject)
	}

	sinfo, err := s.jsmClient.UpdateStream(&nats.StreamConfig{
		Name:     s.opts.ServiceName,
		Subjects: s.subjects,
		NoAck:    false,
	})

	//fmt.Printf("Stream Config Err: %v \n", err)
	if err == nil {
		fmt.Printf("Stream Config: %v \n", sinfo.Config)
	}
	return nil // could be nil.
}
