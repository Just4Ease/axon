package options

import "context"

type PublisherOptions struct {
	contentType string
	ctx         context.Context
}

func (p *PublisherOptions) SetContentType(contentType string) *PublisherOptions {
	p.contentType = contentType
	return p
}

func (p *PublisherOptions) SetContext(ctx context.Context) *PublisherOptions {
	p.ctx = ctx
	return p
}

func (p *PublisherOptions) GetContext() context.Context {
	return p.ctx
}

func (p *PublisherOptions) GetContentType() string {
	return p.contentType
}

func NewPublisherOptions() *PublisherOptions {
	po := &PublisherOptions{}
	po.SetContext(context.Background())
	return po
}

// MergePublisherOptions combines the given PublisherOptions instances into a single
// PublisherOptions in a last-one-wins fashion.
func MergePublisherOptions(opts ...*PublisherOptions) *PublisherOptions {
	po := NewPublisherOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if opt.contentType != "" {
			po.contentType = opt.contentType
		}
		if opt.ctx != nil {
			po.ctx = opt.ctx
		}
	}

	return po
}
