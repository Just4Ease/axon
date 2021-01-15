package stand

import (
	"github.com/Just4Ease/axon"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type stanEvent struct {
	m *stan.Msg
}

func (s stanEvent) Ack() {
	_ = s.m.Ack()
}

func (s stanEvent) Data() []byte {
	return s.m.Data
}

func (s stanEvent) Topic() string {
	return s.m.Subject
}

func newEvent(msg *stan.Msg) axon.Event {
	return &stanEvent{
		m: msg,
	}
}


type natsEvent struct {
	m *nats.Msg
}

func (n natsEvent) Ack() {
	return
}

func (n natsEvent) Data() []byte {
	return n.m.Data
}

func (n natsEvent) Topic() string {
	return n.m.Subject
}

func newNatsEvent(msg *nats.Msg) axon.Event {
	return &natsEvent{
		m: msg,
	}
}