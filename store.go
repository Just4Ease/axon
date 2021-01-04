package axon

import (
	"context"
	"github.com/pkg/errors"
	"log"
	"time"
)

type SubscriptionHandler func(event Event)
type ReplyHandler func(input []byte) ([]byte, error)
type EventHandler func() error

var (
	ErrEmptyStoreName          = errors.New("Sorry, you must provide a valid store name")
	ErrInvalidURL              = errors.New("Sorry, you must provide a valid store URL")
	ErrInvalidTlsConfiguration = errors.New("Sorry, you have provided an invalid tls configuration")
	ErrCloseConn               = errors.New("connection closed")
)

type EventStore interface {
	Publish(topic string, message []byte) error
	Subscribe(topic string, handler SubscriptionHandler) error
	Request(requestURI string, payload []byte, v interface{}) error
	Reply(topic string, handler ReplyHandler) error
	GetServiceName() string
	Run(ctx context.Context, handlers ...EventHandler)
}



func (f EventHandler) Run() {
	for {
		err := f()
		if err != nil {
			log.Printf("creating a consumer returned error: %v. Reconnecting in 3secs...", err)
			time.Sleep(3 * time.Second)
			continue
		}
	}
}
