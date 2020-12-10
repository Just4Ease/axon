package axon

import "encoding/json"
import "github.com/pkg/errors"

type ReplyPayload struct {
	ErrorMessage string          `json:"error_message"`
	Payload      json.RawMessage `json:"payload"`
}

func NewReply(payload []byte, err error) *ReplyPayload {
	return &ReplyPayload{
		Payload:      payload,
		ErrorMessage: errors.WithStack(err).Error(),
	}
}

func (r *ReplyPayload) Compact() ([]byte, error) {
	return json.Marshal(r)
}

func (r *ReplyPayload) GetError() error {
	if r.ErrorMessage != "" {
		return errors.New(r.ErrorMessage)
	}
	return nil
}

func (r *ReplyPayload) GetPayload() []byte {
	return r.Payload
}
