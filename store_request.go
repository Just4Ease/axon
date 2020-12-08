package axon

import (
	"encoding/json"
	"fmt"
)

type RequestPayload struct {
	ReplyPipe string `json:"reply_pipe"`
	Payload   []byte `json:"payload"`
}

func NewRequestPayload(topic string, message []byte) *RequestPayload {
	replyPipe := fmt.Sprintf("%s::%s", topic, GenerateRandomString()) // TODO: Generate Randomness for reply pipe.
	return &RequestPayload{
		ReplyPipe: replyPipe,
		Payload:   message,
	}
}

func (r *RequestPayload) GetReplyAddress() string {
	return r.ReplyPipe
}

func (r *RequestPayload) GetPayload() []byte {
	return r.Payload
}

func (r *RequestPayload) ParsePayload(v interface{}) error {
	return json.Unmarshal(r.Payload, v) // Change to ioutils.readAll not json buffer thingy.
}

func (r *RequestPayload) Compact() ([]byte, error) {
	return json.Marshal(r)
}
