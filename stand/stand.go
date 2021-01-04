package stand

import (
	"context"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/nats-io/stan.go"
	"strings"
)

type natsStore struct {
	client      stan.Conn
	serviceName string
	//opts        axon.Options
}

func (s *natsStore) Publish(topic string, message []byte) error {
	return s.client.Publish(topic, message)
}

func (s *natsStore) Subscribe(topic string, handler axon.SubscriptionHandler) error {
	_, err := s.client.QueueSubscribe(topic, s.serviceName, func(msg *stan.Msg) {
		event := newEvent(msg)
		go handler(event)
	}, stan.DurableName(s.serviceName), stan.SetManualAckMode())
	if err != nil {
		return err
	}
	return nil
}

func (s *natsStore) Request(requestURI string, payload []byte, v interface{}) error {
	panic("implement me")
}

func (s *natsStore) Reply(topic string, handler axon.ReplyHandler) error {
	_, err := s.client.QueueSubscribe(topic, s.serviceName, func(msg *stan.Msg) {
		event := newEvent(msg)

		pl := axon.NewRequestPayload(topic, event.Data())
		out, err := handler(pl.GetPayload())
		rl := axon.NewReply(out, err)

		//err := msg.Reply(pl.GetReplyAddress(), rl.GetPayload())
		_ = s.client.Publish(pl.GetReplyAddress(), rl.GetPayload())
	}, stan.DurableName(s.serviceName), stan.SetManualAckMode())
	if err != nil {
		return err
	}
	return nil
}

func (s *natsStore) GetServiceName() string {
	return s.serviceName
}

func Init(opts axon.Options, options ...stan.Option) (axon.EventStore, error) {

	addr := strings.TrimSpace(opts.Address)
	if addr == "" {
		return nil, axon.ErrInvalidURL
	}

	//name := strings.TrimSpace(opts.ServiceName)
	//if name == "" {
	//	return nil, axon.ErrEmptyStoreName
	//}
	//
	//clientOptions := pulsar.ClientOptions{URL: addr}
	//if opts.CertContent != "" {
	//	certPath, err := initCert(opts.CertContent)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	//if opts.AuthenticationToken != "" {
	//	clientOptions.Authentication = pulsar.NewAuthenticationToken(opts.AuthenticationToken)
	//}
	//
	//rand.Seed(time.Now().UnixNano())
	//p, err := pulsar.NewClient(clientOptions)
	//if err != nil {
	//	return nil, fmt.Errorf("unable to connect with Pulsar with provided configuration. failed with error: %v", err)
	//}

	st, err := stan.Connect(opts.Address, opts.ServiceName, options...)
	if err != nil {
		return nil, fmt.Errorf("unable to connect with NATS with the provided configuration. failed with error: %v", err)
	}

	return &natsStore{
		client:      st,
		serviceName: opts.ServiceName,
	}, nil
}

func (s *natsStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}

	<-ctx.Done()
}
