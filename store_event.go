package axon

type Event interface {
	Ack()
	Data() []byte
	Topic() string
}
