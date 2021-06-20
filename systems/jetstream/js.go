package jetstream

import (
	"context"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"log"
	"strings"
	"time"
)

const Empty = ""

type natsStore struct {
	codec.Codec
	opts       options.Options
	natsClient *nats.Conn
	jsmClient  nats.JetStreamContext
}

func (s *natsStore) newCodec(contentType string) (codec.NewCodec, error) {
	if c, ok := s.opts.Codecs[contentType]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("unsupported Content-Type: %s", contentType)
}

//func (s *natsStore) Publish(topic string, message []byte) error {
//	var sub stan.Subscription
//	var err error
//
//	so := options.MergeSubscriptionOptions(opts...)
//
//
//	return s.stanClient.Publish(topic, message)
//}

//func (s *natsStore) Publish(topic string, message []byte) error {
//
//
//
//
//	return s.stanClient.Publish(topic, message)
//}

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
	var sub *nats.Subscription
	var err error

	var cancel context.CancelFunc
	var ctx context.Context

	so := options.MergeSubscriptionOptions(opts...)
	if so.GetContext() != nil {
		ctx, cancel = context.WithCancel(so.GetContext())
		so.SetContext(ctx)
	}

	subType := so.GetSubscriptionType()
	if subType == options.Shared {
		sub, err = s.jsmClient.QueueSubscribe(topic, s.opts.ServiceName, func(m *nats.Msg) {

			var msg messages.Message
			if err = s.opts.Unmarshal(m.Data, &msg); err != nil {
				cancel()
				return
			}

			event := newEvent(m, msg)
			go handler(event)
		}, nats.Durable(s.opts.ServiceName), nats.ManualAck(), nats.EnableFlowControl())
	}

	if subType == options.KeyShared {
		sub, err = s.jsmClient.Subscribe(topic, func(m *nats.Msg) {
			var msg messages.Message
			if err = s.opts.Unmarshal(m.Data, &msg); err != nil {
				cancel()
				return
			}

			event := newEvent(m, msg)
			go handler(event)
		}, nats.Durable(s.opts.ServiceName), nats.ManualAck())
	}

	defer cancel()

	select {
	case <-so.GetContext().Done():
		return err
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
	msg, err := nc.Request(message.Subject, data, time.Second)
	if err != nil {
		log.Print("error making request: ", err)
		if err == nats.ErrConnectionClosed {

		}
		return nil, err
	}

	var mg messages.Message
	if err := s.opts.Unmarshal(msg.Data, &mg); err != nil {
		log.Print("failed to unmarshal reply event into reply struct with the following errors: ", err)
		_ = msg.Nak()
		return nil, err
	}

	_ = msg.Ack()
	// Check if reply has an issue.
	if mg.Type == messages.ErrorMessage {
		return &mg, errors.New(mg.Error)
	}

	return &mg, nil
}

//
func (s *natsStore) Reply(topic string, handler axon.ReplyHandler) error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		_, err := s.jsmClient.QueueSubscribe(topic, s.opts.ServiceName, func(msg *nats.Msg) {
			var mg messages.Message
			if err := s.opts.Unmarshal(msg.Data, &mg); err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				return
			}

			responseMessage, err := handler(&mg)
			if err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				responseMessage = messages.NewMessage()
				responseMessage.Error = err.Error()
				responseMessage.WithType(messages.ErrorMessage)
				return
			} else {
				responseMessage.WithType(messages.ResponseMessage)
			}
			responseMessage.WithSpecVersion(mg.SpecVersion)
			responseMessage.WithSource(s.opts.ServiceName)
			responseMessage.WithSubject(topic)

			data, err := s.opts.Marshal(responseMessage)
			if err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				return
			}

			if err := msg.Respond(data); err != nil {
				log.Print("failed to reply data to the incoming request with the following error: ", err)
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

func Init(opts options.Options, clusterId string, options ...stan.Option) (axon.EventStore, error) {

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

	nc, err := nats.Connect(opts.Address, nats.Name(opts.ServiceName))
	if err != nil {
		return nil, err
	}

	var optsList []stan.Option
	optsList = append(optsList, stan.NatsConn(nc))
	optsList = append(optsList, options...)
	//st, err := stan.Connect(clusterId, fmt.Sprintf("%s-%s", opts.ServiceName, utils.GenerateRandomString()), optsList...)
	//if err != nil {
	//	return nil, fmt.Errorf("unable to connect with NATS with the provided configuration. failed with error: %v", err)
	//}

	js, _ := nc.JetStream()

	return &natsStore{
		jsmClient:  js,
		natsClient: nc,
		opts:       opts,
	}, nil
}

func (s *natsStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}

	<-ctx.Done()
}
