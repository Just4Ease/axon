package axon

import "encoding/json"

type ReplyPayload struct {
	err     error
	payload []byte
}

func NewReply(payload []byte, err error) *ReplyPayload {
	return &ReplyPayload{
		payload: payload,
		err:     err,
	}
}

func (r *ReplyPayload) Compact() ([]byte, error) {
	return json.Marshal(r)
}

func (r *ReplyPayload) GetError() error {
	return r.err
}

func (r *ReplyPayload) GetPayload() []byte {
	return r.payload
}
