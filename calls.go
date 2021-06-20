package axon

import (
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/utils"
)
import "github.com/pkg/errors"

type ReplyPayload struct {
	ErrorMessage string          `json:"error_message"`
	Payload      json.RawMessage `json:"payload"`
}

func NewReply(payload []byte, err error) *ReplyPayload {
	s := ""
	if err != nil {
		s = errors.WithStack(err).Error()
	}
	return &ReplyPayload{
		Payload:      payload,
		ErrorMessage: s,
	}
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

type RequestPayload struct {
	ReplyPipe string `json:"reply_pipe"`
	Payload   *messages.Message `json:"payload"`
}

func NewRequestPayload(topic string, message *messages.Message) *RequestPayload {
	replyPipe := fmt.Sprintf("%s::%s", topic, utils.GenerateRandomString()) // TODO: Generate Randomness for reply pipe.
	return &RequestPayload{
		ReplyPipe: replyPipe,
		Payload:   message,
	}
}

func (r *RequestPayload) GetReplyAddress() string {
	return r.ReplyPipe
}

func (r *RequestPayload) GetPayload() []byte {
	return r.Payload.Body
}
