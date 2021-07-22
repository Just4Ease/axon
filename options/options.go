package options

import (
	"context"
	"github.com/Just4Ease/axon/v2/codec"
	"github.com/Just4Ease/axon/v2/codec/msgpack"
	"github.com/prometheus/common/log"
)

type Options struct {
	// Used to select codec
	ServiceName         string
	Address             string
	CertContent         string
	AuthenticationToken string
	codec.Marshaler

	// Default Call Options
	//CallOptions CallOptions

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

func (o *Options) EnsureDefaultMarshaling() {
	if o.Marshaler == nil {
		o.Marshaler = msgpack.Marshaler{}
		log.Info("custom marshaler not found. defaulting to msgpack")
	}
}

//var (
//	DefaultContentType = "application/msgpack"
//
//	DefaultCodecs = map[string]codec.NewCodec{
//		"application/json":         json.NewCodec,
//		"application/msgpack":      msgpack.NewCodec,
//		"application/protobuf":     proto.NewCodec,
//		"application/octet-stream": raw.NewCodec,
//	}
//)
