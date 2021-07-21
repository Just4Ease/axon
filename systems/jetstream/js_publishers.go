package jetstream

import (
	"errors"
	"github.com/Just4Ease/axon/messages"
	"strings"
)

func (s *natsStore) Publish(message *messages.Message) error {
	if message == nil {
		return errors.New("invalid message")
	}

	message.WithType(messages.EventMessage)
	data, err := s.opts.Marshal(message)
	if err != nil {
		return err
	}

	if strings.TrimSpace(message.Subject) == empty {
		return errors.New("invalid topic name")
	}

	s.mountAndRegisterPublishTopics(message.Subject)

	_, err = s.jsmClient.Publish(message.Subject, data)
	return err
}

func (s *natsStore) mountAndRegisterPublishTopics(topic string) {
	s.mu.Lock()
	if _, ok := s.publishTopics[topic]; ok {
		s.mu.Unlock()
		return
	}

	if _, ok := s.subscriptions[topic]; ok {
		s.mu.Unlock()
		return
	}

	s.publishTopics[topic] = topic
	s.mu.Unlock()

	s.registerSubjectsOnStream()
}
