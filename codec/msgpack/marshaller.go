package msgpack

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Marshaler struct{}

func (Marshaler) Marshal(v interface{}) ([]byte, error) {
	enc := msgpack.GetEncoder()

	enc.SetCustomStructTag("json")

	var buf bytes.Buffer
	enc.Reset(&buf)

	err := enc.Encode(v)
	b := buf.Bytes()

	msgpack.PutEncoder(enc)

	if err != nil {
		return nil, err
	}
	return b, err
}

func (Marshaler) Unmarshal(d []byte, v interface{}) error {
	if pb, ok := v.(proto.Message); ok {
		return jsonpb.Unmarshal(d, pb)
	}

	dec := msgpack.GetDecoder()

	dec.SetCustomStructTag("json")

	dec.Reset(bytes.NewReader(d))
	err := dec.Decode(v)

	msgpack.PutDecoder(dec)

	if err != nil {
		return err
	}
	return nil
}

func (Marshaler) String() string {
	return "msgpack"
}
