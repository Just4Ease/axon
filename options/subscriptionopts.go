package options

import "context"

// SubscriptionType of subscription supported by most messaging systems. ( Pulsar,
type SubscriptionType int

const (
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = iota

	// Shared subscription mode, multiple consumer will be able to use the same subscription name
	// and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared

	// Failover subscription mode, multiple consumer will be able to use the same subscription name
	// but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	// KeyShared subscription mode, multiple consumer will be able to use the same
	// subscription and all messages with the same key will be dispatched to only one consumer
	KeyShared
)

type SubscriptionOptions struct {
	subscriptionType *SubscriptionType
	contentType      string
	ctx              context.Context
	maxReDelivery    int
}

func (s *SubscriptionOptions) SetContentType(contentType string) *SubscriptionOptions {
	s.contentType = contentType
	return s
}

func (s *SubscriptionOptions) SetContext(ctx context.Context) *SubscriptionOptions {
	s.ctx = ctx
	return s
}

func (s *SubscriptionOptions) GetContext() context.Context {
	return s.ctx
}

func (s *SubscriptionOptions) GetContentType() string {
	return s.contentType
}

func (s *SubscriptionOptions) SetSubscriptionType(t SubscriptionType) *SubscriptionOptions {
	s.subscriptionType = &t
	return s
}

func (s *SubscriptionOptions) GetSubscriptionType() SubscriptionType {
	return *s.subscriptionType
}


func (s *SubscriptionOptions) SetMaxRedelivery(n int) *SubscriptionOptions {
	s.maxReDelivery = n
	return s
}

func (s *SubscriptionOptions) GetMaxRedelivery() int {
	return s.maxReDelivery
}

func NewSubscriptionOptions() *SubscriptionOptions {
	so := &SubscriptionOptions{}
	so.SetContext(context.Background())
	so.SetSubscriptionType(Shared)
	so.SetMaxRedelivery(5)
	return so
}

// MergeSubscriptionOptions combines the given SubscriptionOptions instances into a single
// SubscriptionOptions in a last-one-wins fashion.
func MergeSubscriptionOptions(opts ...*SubscriptionOptions) *SubscriptionOptions {
	so := NewSubscriptionOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if opt.subscriptionType != nil {
			so.subscriptionType = opt.subscriptionType
		}
		if opt.contentType != "" {
			so.contentType = opt.contentType
		}
		if opt.ctx != nil {
			so.ctx = opt.ctx
		}
	}

	return so
}
