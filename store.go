package axon

import (
	"context"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/pkg/errors"
	"log"
	"time"
)

type SubscriptionHandler func(event Event)
type ReplyHandler func(mg *messages.Message) (*messages.Message, error)
type EventHandler func() error

func (f EventHandler) Run() {
runLabel:
	if err := f(); err != nil {
		log.Printf("creating a consumer returned error: %v. \nRetrying in 3 seconds", err)
		time.Sleep(time.Second * 3)
		goto runLabel
	}
}

var (
	ErrEmptyStoreName          = errors.New("Sorry, you must provide a valid store name")
	ErrInvalidURL              = errors.New("Sorry, you must provide a valid store URL")
	ErrInvalidTlsConfiguration = errors.New("Sorry, you have provided an invalid tls configuration")
	ErrCloseConn               = errors.New("connection closed")
)

type EventStore interface {
	Publish(message *messages.Message) error
	Subscribe(topic string, handler SubscriptionHandler, opts ...*options.SubscriptionOptions) error
	Request(message *messages.Message) (*messages.Message, error)
	Reply(topic string, handler ReplyHandler) error
	GetServiceName() string
	Run(ctx context.Context, handlers ...EventHandler)
}
