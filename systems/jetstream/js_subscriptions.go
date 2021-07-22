package jetstream

import (
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/nats-io/nats.go"
	"log"
	"strings"
	"time"
)

type subscription struct {
	topic       string
	cb          axon.SubscriptionHandler
	axonOpts    *options.Options
	subOptions  *options.SubscriptionOptions
	jsmClient   nats.JetStreamContext
	serviceName string
}

func (s *subscription) mountSubscription() error {
	errChan := make(chan error)

	closeSub := make(chan bool)

	cbHandler := func(m *nats.Msg) {
		var msg messages.Message
		if err := s.axonOpts.Unmarshal(m.Data, &msg); err != nil {
			errChan <- err
			return
		}

		event := newEvent(m, msg)
		go s.cb(event)
	}
	durableStore := strings.ReplaceAll(fmt.Sprintf("%s-%s", s.serviceName, s.topic), ".", "-")

	go func(s *subscription, closeSub chan bool, errChan chan<- error) {
		var err error
		var sub *nats.Subscription
		go func(sub *nats.Subscription, closeSub chan bool) {
			<-closeSub
			if err := sub.Drain(); err != nil {
				log.Printf("failed to drain subscription with the following error before exitting: %v", err)
			}
		}(sub, closeSub)
		switch s.subOptions.GetSubscriptionType() {
		case options.Failover:

			break
		case options.Exclusive:
			consumer, err := s.jsmClient.AddConsumer(s.serviceName, &nats.ConsumerConfig{
				Durable: durableStore,
				//DeliverSubject: nats.NewInbox(),
				DeliverPolicy: nats.DeliverLastPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				MaxDeliver:    s.subOptions.GetMaxRedelivery(),
				ReplayPolicy:  nats.ReplayOriginalPolicy,
				MaxAckPending: 20000,
				FlowControl:   false,
				//AckWait:         0,
				//RateLimit:       0,
				//Heartbeat:       0,
			})
			if err != nil {
				errChan <- err
				return
			}

			if sub, err = s.jsmClient.QueueSubscribe(consumer.Name, durableStore, cbHandler, nats.Durable(durableStore),
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.serviceName),
				nats.MaxAckPending(20000000),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(s.subOptions.GetMaxRedelivery())); err != nil {
				errChan <- err
				return
			}

		case options.Shared:
			sub, err = s.jsmClient.QueueSubscribe(s.topic,
				durableStore,
				cbHandler,
				nats.Durable(durableStore),
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.serviceName),
				nats.MaxAckPending(20000000),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(s.subOptions.GetMaxRedelivery()))
			if err != nil {
				errChan <- err
				return
			}
		case options.KeyShared:
			if sub, err = s.jsmClient.Subscribe(s.topic,
				cbHandler,
				nats.Durable(durableStore),
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.serviceName),
				nats.MaxAckPending(20000000),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(s.subOptions.GetMaxRedelivery())); err != nil {
				errChan <- err
				return
			}
		}

	}(s, closeSub, errChan)

	log.Printf("subscribed to event channel: %s \n", s.topic)
	select {
	case <-s.subOptions.GetContext().Done():
		closeSub <- true
		return nil
	case err := <-errChan:
		return err
	}
}

func (s *subscription) runSubscriptionHandler() {
start:
	if err := s.mountSubscription(); err != nil {
		log.Printf("creating a consumer returned error: %v. Reconnecting in 3secs...", err)
		time.Sleep(3 * time.Second)
		goto start
	}
}

func (s *natsStore) addSubscriptionToSubscriptionPool(sub *subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.subscriptions[sub.topic]; ok {
		log.Fatalf("there is already an existing subscription registered to this topic: %s", sub.topic)
	}

	s.subscriptions[sub.topic] = sub
	return nil
}

func (s *natsStore) Subscribe(topic string, handler axon.SubscriptionHandler, opts ...*options.SubscriptionOptions) error {
	so := options.MergeSubscriptionOptions(opts...)
	sub := &subscription{
		topic:       topic,
		cb:          handler,
		axonOpts:    &s.opts,
		subOptions:  so,
		jsmClient:   s.jsmClient,
		serviceName: s.opts.ServiceName,
	}
	return s.addSubscriptionToSubscriptionPool(sub)
}
