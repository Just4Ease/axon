package liftbridged

import (
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	raw "github.com/Just4Ease/axon/codec/bytes"
	"github.com/Just4Ease/axon/codec/json"
	"github.com/Just4Ease/axon/codec/msgpack"
	"github.com/Just4Ease/axon/messages"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	mp "github.com/vmihailenco/msgpack/v5"
)

type lftEvent struct {
	m     *lift.Message
	msg   messages.Message
	codec map[string]codec.NewCodec
}

func (l lftEvent) Parse(value interface{}) (*messages.Message, error) {
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
	if err := mp.Unmarshal(l.msg.Body, value); err != nil {
		return nil, err
	}

	return &l.msg, nil
}

func (l lftEvent) Ack() {}

func (l lftEvent) NAck() {}

func (l lftEvent) Data() []byte {
	return l.m.Value()
}

func (l lftEvent) Topic() string {
	return l.m.Subject()
}

func newEvent(m *lift.Message, msg messages.Message) axon.Event {
	return &lftEvent{
		m:   m,
		msg: msg,
		codec: map[string]codec.NewCodec{
			"application/json":         json.NewCodec,
			"application/msgpack":      msgpack.NewCodec,
			"application/octet-stream": raw.NewCodec,
		},
	}
}
