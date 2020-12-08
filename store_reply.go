package axon

import "encoding/json"

type ReplyPayload struct {
	Err     error `json:"err"`
	Payload []byte `json:"payload"`
}

func NewReply(payload []byte, err error) *ReplyPayload {
	return &ReplyPayload{
		Payload: payload,
		Err:     err,
	}
}

func (r *ReplyPayload) Compact() ([]byte, error) {
	return json.Marshal(r)
}

func (r *ReplyPayload) GetError() error {
	return r.Err
}

func (r *ReplyPayload) GetPayload() []byte {
	return r.Payload
}
