package stand

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/nats-io/stan.go"
	"log"
	"strings"
)

type natsStore struct {
	client      stan.Conn
	serviceName string
}

func (s *natsStore) Publish(topic string, message []byte) error {
	return s.client.Publish(topic, message)
}

func (s *natsStore) Subscribe(topic string, handler axon.SubscriptionHandler) error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		_, err := s.client.QueueSubscribe(topic, s.serviceName, func(msg *stan.Msg) {
			event := newEvent(msg)
			go handler(event)
		}, stan.DurableName(s.serviceName), stan.SetManualAckMode())
		if err != nil {
			errChan <- err
		}

		<-make(chan bool) // Sig Wait.
	}(errChan)

	return <-errChan
}

func (s *natsStore) Request(requestURI string, payload []byte, v interface{}) error {
	errChan := make(chan error)
	eventChan := make(chan axon.Event)
	req := axon.NewRequestPayload(requestURI, payload)
	go func(errChan chan<- error, eventChan chan<- axon.Event, req *axon.RequestPayload, conn stan.Conn) {
		doneSig := make(chan bool)
		sub, err := s.client.QueueSubscribe(req.ReplyPipe, s.serviceName, func(msg *stan.Msg) {
			event := newEvent(msg)
			eventChan <- event
			doneSig <- true
		}, stan.DurableName(s.serviceName), stan.SetManualAckMode())
		if err != nil {
			errChan <- err
		}

		defer func(errChan chan<- error) {
			if err := sub.Unsubscribe(); err != nil {
				errChan <- err
			}
		}(errChan)

		ok := <-doneSig
		if ok {
			return
		}
	}(errChan, eventChan, req, s.client)

	go func(errChan chan<- error, payload *axon.RequestPayload, topic string) {
		data, err := payload.Compact()
		if err != nil {
			log.Printf("failed to compact request of: %s for transfer with the following errors: %v", topic, err)
			errChan <- err
			return
		}

		if err := s.Publish(topic, data); err != nil {
			log.Printf("failed to send request for: %s with the following errors: %v", topic, err)
			errChan <- err
		}
	}(errChan, req, requestURI)
	// Read address from
	for {
		select {
		case err := <-errChan:
			log.Print("failed to receive reply-response with the following errors: ", err)
			return err
		default:
			event := <-eventChan

			// This is the ReplyPayload
			var reply axon.ReplyPayload
			if err := json.Unmarshal(event.Data(), &reply); err != nil {
				log.Print("failed to unmarshal reply event into reply struct with the following errors: ", err)
				event.Ack()
				return err
			}

			// Check if reply has an issue.
			if replyErr := reply.GetError(); replyErr != nil {
				event.Ack()
				return replyErr
			}

			// Unpack Reply's payload.
			if err := json.Unmarshal(reply.GetPayload(), v); err != nil {
				log.Print("failed to unmarshal reply payload into struct with the following errors: ", err)
				event.Ack()
				return err
			}

			event.Ack()
			return nil
		}
	}
}

func (s *natsStore) Reply(topic string, handler axon.ReplyHandler) error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		_, err := s.client.QueueSubscribe(topic, s.serviceName, func(msg *stan.Msg) {
			event := newEvent(msg)
			var reqPl axon.RequestPayload
			decoder := json.NewDecoder(bytes.NewBuffer(event.Data()))
			decoder.UseNumber()
			if err := decoder.Decode(&reqPl); err != nil {
				log.Print("failed to decode incoming request payload []bytes with the following error: ", err)
				return
			}

			out, err := handler(reqPl.GetPayload())
			rl := axon.NewReply(out, err)

			data, err := rl.Compact()
			if err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				return
			}

			if err := s.Publish(reqPl.GetReplyAddress(), data); err != nil {
				log.Print("failed to reply data to the incoming request with the following error: ", err)
				return
			}

			event.Ack()
		}, stan.DurableName(s.serviceName), stan.SetManualAckMode())
		if err != nil {
			errChan <- err
		}
	}(errChan)
	return <-errChan
}

func (s *natsStore) GetServiceName() string {
	return s.serviceName
}

func Init(opts axon.Options, clusterId string, options ...stan.Option) (axon.EventStore, error) {

	addr := strings.TrimSpace(opts.Address)
	if addr == "" {
		return nil, axon.ErrInvalidURL
	}

	name := strings.TrimSpace(opts.ServiceName)
	if name == "" {
		return nil, axon.ErrEmptyStoreName
	}
	//if opts.AuthenticationToken != "" {
	//	clientOptions.Authentication = pulsar.NewAuthenticationToken(opts.AuthenticationToken)
	//}

	var optsList []stan.Option
	optsList = append(optsList, stan.NatsURL(opts.Address))
	optsList = append(optsList, options...)
	st, err := stan.Connect(clusterId, name, optsList...)
	if err != nil {
		return nil, fmt.Errorf("unable to connect with NATS with the provided configuration. failed with error: %v", err)
	}

	return &natsStore{
		client:      st,
		serviceName: name,
	}, nil
}

func (s *natsStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}

	<-ctx.Done()
}
