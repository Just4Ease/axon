package axon

import "github.com/Just4Ease/axon/messages"

type Event interface {
	Ack()
	NAck()
	Parse(value interface{}) (*messages.Message, error)
	Data() []byte
	Topic() string
}
