package stand

import (
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	raw "github.com/Just4Ease/axon/codec/bytes"
	"github.com/Just4Ease/axon/codec/json"
	"github.com/Just4Ease/axon/codec/msgpack"
	"github.com/Just4Ease/axon/messages"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	mp "github.com/vmihailenco/msgpack/v5"
)

type stanEvent struct {
	m *stan.Msg
	//codec.Marshaler
	msg   messages.Message
	codec map[string]codec.NewCodec
}

func (s stanEvent) Parse(value interface{}) (*messages.Message, error) {
	//nc, ok := s.codec[s.msg.ContentType.String()]
	//if !ok {
	//	return nil, errors.New("unsupported payload codec")
	//}

	//rwc := codec.NewReadWriteCloser(bufferPool)
	////if _, err := rwc.Write(s.msg.Body); err != nil {
	////	return nil, err
	////}
	//
	//
	//cc := nc(rwc)
	//
	//if err := cc.Write(s.msg.Body); err != nil {
	//	return nil, err
	//}
	//
	//fmt.Printf( " Collected message codec %s", cc)
	//
	//
	//if err := cc.Read(value); err != nil {
	//	return nil, err
	//}
	if err := mp.Unmarshal(s.msg.Body, value); err != nil {
		return nil, err
	}

	return &s.msg, nil
}

func (s stanEvent) Ack() {
	_ = s.m.Ack()
}

func (s stanEvent) NAck() {
	return
}

func (s stanEvent) Data() []byte {
	return s.m.Data
}

func (s stanEvent) Topic() string {
	return s.m.Subject
}

func newEvent(m *stan.Msg, msg messages.Message) axon.Event {
	return &stanEvent{
		m:   m,
		msg: msg,
		codec: map[string]codec.NewCodec{
			"application/json":         json.NewCodec,
			"application/msgpack":      msgpack.NewCodec,
			"application/octet-stream": raw.NewCodec,
		},
	}
}

type natsEvent struct {
	m *nats.Msg
}

func (n natsEvent) Parse(value interface{}) (*messages.Message, error) {
	panic("implement me")
}

func (n natsEvent) Ack() {
	return
}

func (n natsEvent) NAck() {

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
