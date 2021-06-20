package liftbridged

import (
	"context"
	"errors"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"log"
)

const Empty = ""

type lftStore struct {
	opts   options.Options
	client lift.Client
}

func (l lftStore) Publish(message *messages.Message) error {
	if message == nil {
		return errors.New("invalid message")
	}

	message.WithType(messages.EventMessage)
	data, err := l.opts.Marshal(message)
	if err != nil {
		return err
	}

	lMSG := lift.NewMessage(data, lift.AckPolicyAll())

	if _, err := l.client.Publish(l.opts.Context, message.Subject, lMSG); err != nil {
		return err
	}
	return nil
}

func (l lftStore) Subscribe(topic string, handler axon.SubscriptionHandler, opts ...*options.SubscriptionOptions) error {
	errChan := make(chan error)

	//subType := so.GetSubscriptionType()
	var cancel context.CancelFunc
	var ctx context.Context

	so := options.MergeSubscriptionOptions(opts...)
	ctx, cancel = context.WithCancel(so.GetContext())
	so.SetContext(ctx)

	go func(errChan chan error, msh codec.Marshaler, ctx context.Context) {

		if err := l.client.CreateStream(ctx, topic, l.opts.ServiceName); err != nil {
			errChan <- err
			return
		}

		err := l.client.Subscribe(so.GetContext(), topic, func(m *lift.Message, err error) {
			var msg messages.Message

			if err = msh.Unmarshal(m.Value(), &msg); err != nil {
				errChan <- err
				return
			}
			ev := newEvent(m, msg)
			go handler(ev)
		},
			lift.StartAtLatestReceived(),
			lift.Resume(),
			lift.ReadISRReplica(),
		)

		if err != nil {
			errChan <- err
		}
	}(errChan, l.opts.Marshaler, so.GetContext())

	defer cancel()

	select {
	case <-so.GetContext().Done():
		return nil
	case err := <-errChan:
		return err
	}
}

func buildUniqueSubject(subject, id string) string {
	return fmt.Sprintf("%s-%s", subject, id)
}

func (l lftStore) Request(message *messages.Message) (*messages.Message, error) {
	message.WithType(messages.RequestMessage)
	message.WithSource(l.opts.ServiceName)
	if message.SpecVersion == Empty {
		message.WithSpecVersion("default")
	}

	errChan := make(chan error)
	responseMessage := make(chan *messages.Message)

	go func(responseMessage chan *messages.Message, errChan chan error, opts *options.Options) {
		if err := l.client.Subscribe(opts.Context, buildUniqueSubject(message.Subject, message.Id), func(m *lift.Message, err error) {
			var msg messages.Message
			if err := opts.Unmarshal(m.Value(), &msg); err != nil {
				errChan <- err
				return
			}
			responseMessage <- &msg
		}); err != nil {
			errChan <- err
		}
	}(responseMessage, errChan, &l.opts)

	data, err := l.opts.Marshal(message)
	if err != nil {
		return nil, err
	}

	lMSG := lift.NewMessage(data, lift.AckPolicyNone())
	if _, err := l.client.Publish(l.opts.Context, message.Subject, lMSG); err != nil {
		log.Print("error making request: ", err)
		return nil, err
	}

	select {
	case err := <-errChan:
		return nil, err

	case msg := <-responseMessage:
		// Check if reply has an issue.
		if msg.Type == messages.ErrorMessage {
			return msg, errors.New(msg.Error)
		}

		return msg, nil
	}
}

func (l lftStore) Reply(topic string, handler axon.ReplyHandler) error {
	errChan := make(chan error)
	go func(opts *options.Options, errChan chan<- error) {
		err := l.client.Subscribe(l.opts.Context, topic, func(mg *lift.Message, err error) {

			if err != nil {
				errChan <- err
				return
			}

			var msg messages.Message
			if err := l.opts.Unmarshal(mg.Value(), &msg); err != nil {
				//log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				errChan <- err
				return
			}

			responseMessage, err := handler(&msg)
			if err != nil {
				// This error isn't sent to the errChan, this is just the error returned from the handler for any failed command in the handling side according to their business logic.
				responseMessage = messages.NewMessage()
				responseMessage.Error = err.Error()
				responseMessage.WithType(messages.ErrorMessage)
				return
			} else {
				responseMessage.WithType(messages.ResponseMessage)
			}
			responseMessage.WithSpecVersion(msg.SpecVersion)
			responseMessage.WithSource(opts.ServiceName)
			responseMessage.WithSubject(topic)

			data, err := opts.Marshal(responseMessage)
			if err != nil {
				//log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				errChan <- err
				return
			}

			//s.opts.ServiceName,

			//if err := msg.Respond(data); err != nil {
			//	log.Print("failed to reply data to the incoming request with the following error: ", err)
			//	return
			//}
			lMSG := lift.NewMessage(data, lift.AckPolicyNone())
			if _, err := l.client.Publish(l.opts.Context, buildUniqueSubject(msg.Subject, msg.Id), lMSG); err != nil {
				log.Print("error replying request: ", err)
				errChan <- err
				return
			}
		})
		if err != nil {
			errChan <- err
		}
	}(&l.opts, errChan)
	return <-errChan
}

func (l lftStore) GetServiceName() string {
	return l.opts.ServiceName
}

func (l lftStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}

	<-ctx.Done()
}

func Init(opts options.Options) (axon.EventStore, error) {
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := lift.Connect(addrs)
	if err != nil {
		return nil, err
	}

	l := &lftStore{client: client, opts: opts}

	return l, nil
}
