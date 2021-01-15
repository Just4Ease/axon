package stand

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"log"
	"strings"
	"time"
)

type natsStore struct {
	stanClient  stan.Conn
	natsClient  *nats.Conn
	serviceName string
}

func (s *natsStore) Publish(topic string, message []byte) error {
	return s.stanClient.Publish(topic, message)
}

func (s *natsStore) Subscribe(topic string, handler axon.SubscriptionHandler) error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		_, err := s.stanClient.QueueSubscribe(topic, s.serviceName, func(msg *stan.Msg) {
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
	nc := s.natsClient

	req := axon.NewRequestPayload(requestURI, payload)
	data, err := req.Compact()
	msg, err := nc.Request(requestURI, data, time.Second*1)
	if err != nil {
		log.Print("Error making eventful request: ", err)
		return err
	}

	event := newNatsEvent(msg)
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

func (s *natsStore) Reply(topic string, handler axon.ReplyHandler) error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		_, err := s.natsClient.QueueSubscribe(topic, s.serviceName, func(msg *nats.Msg) {
			event := newNatsEvent(msg)
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

	nc, err := nats.Connect(opts.Address, nats.Name(name))
	if err != nil {
		return nil, err
	}

	var optsList []stan.Option
	optsList = append(optsList, stan.NatsConn(nc))
	optsList = append(optsList, options...)
	st, err := stan.Connect(clusterId, fmt.Sprintf("%s-%s", name, axon.GenerateRandomString()), optsList...)
	if err != nil {
		return nil, fmt.Errorf("unable to connect with NATS with the provided configuration. failed with error: %v", err)
	}

	return &natsStore{
		stanClient:  st,
		natsClient:  nc,
		serviceName: name,
	}, nil
}

func (s *natsStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}

	<-ctx.Done()
}
