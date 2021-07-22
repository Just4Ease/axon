package jetstream

import (
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

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
		log.Printf("failed to unmarshal reply event into reply struct with the following errors: %v", err)
		_ = msg.Term()
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
				log.Printf("failed to encode reply payload into Message{} with the following error: %v", err)
				errChan <- err
				return
			}

			responseMessage, responseError := handler(&mg)
			if responseError != nil {
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
