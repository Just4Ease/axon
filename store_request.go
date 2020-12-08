package axon

import (
	"encoding/json"
	"fmt"
)

type RequestPayload struct {
	replyPipe string
	payload   []byte
}

func NewRequestPayload(topic string, message []byte) *RequestPayload {
	replyPipe := fmt.Sprintf("%s::%s", topic, GenerateRandomString()) // TODO: Generate Randomness for reply pipe.
	return &RequestPayload{
		replyPipe: replyPipe,
		payload:   message,
	}
}

func (r *RequestPayload) GetReplyAddress() string {
	return r.replyPipe
}

func (r *RequestPayload) GetPayload() []byte {
	return r.payload
}

func (r *RequestPayload) ParsePayload(v interface{}) error {
	return json.Unmarshal(r.payload, v) // Change to ioutils.readAll not json buffer thingy.
}

func (r *RequestPayload) Compact() ([]byte, error) {
	return json.Marshal(r)
}
