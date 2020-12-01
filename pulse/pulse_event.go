package pulse

import (
	"github.com/Just4Ease/axon"
)

type event struct {
	raw      axon.Message
	consumer axon.Consumer
}

func NewEvent(message axon.Message, consumer axon.Consumer) axon.Event {
	return &event{raw: message, consumer: consumer}
}

func (e *event) Data() []byte {
	return e.raw.Payload()
}

func (e *event) Topic() string {
	t := e.raw.Topic()
	// Manually agree what we want the topic to look like from pulsar.
	return t
}

func (e *event) Ack() {
	e.consumer.Ack(e.raw.ID())
}
