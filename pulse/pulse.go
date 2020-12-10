package pulse

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/apache/pulsar-client-go/pulsar"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type pulsarStore struct {
	serviceName string
	client      axon.Client
	opts        axon.Options
}

func (s *pulsarStore) Reply(topic string, handler axon.ReplyHandler) error {
	serviceName := s.GetServiceName()
	var consumer axon.Consumer
	var err error
	if consumer, err = s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		AutoDiscoveryPeriod:         0,
		SubscriptionName:            fmt.Sprintf("%s-%s", serviceName, topic),
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		Name:                        serviceName,
	}); err != nil {
		return fmt.Errorf("error subscribing to topic. %v", err)
	}

	defer consumer.Close()
	for {
		message, err := consumer.Recv(context.Background())
		if err != nil {
			if err == axon.ErrCloseConn {
				break
			}
			continue
		}

		event := NewEvent(message, consumer)
		go func(event axon.Event) {
			var reqPl axon.RequestPayload
			decoder := json.NewDecoder(bytes.NewBuffer(event.Data()))
			decoder.UseNumber()
			if err := decoder.Decode(&reqPl); err != nil {
				log.Print("failed to decode incoming request payload []bytes with the following error: ", err)
				return
			}
			// Execute Handler
			handlerPayload, handlerError := handler(reqPl)
			replyPayload := axon.NewReply(handlerPayload, handlerError)
			data, err := replyPayload.Compact()
			if err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				return
			}

			if err := s.Publish(reqPl.GetReplyAddress(), data); err != nil {
				log.Print("failed to reply data to the incoming request with the following error: ", err)
				return
			}

			event.Ack()
		}(event)
	}
	return nil
}

func (s *pulsarStore) Request(topic string, message []byte, v interface{}) error {
	errChan := make(chan error, 1)
	eventChan := make(chan axon.Event, 1)
	req := axon.NewRequestPayload(topic, message)
	go func(errChan chan<- error, eventChan chan<- axon.Event, replyAddress string) {
		serviceName := s.GetServiceName()
		consumer, err := s.client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       replyAddress,
			AutoDiscoveryPeriod:         0,
			SubscriptionName:            fmt.Sprintf("%s-%s", serviceName, topic),
			Type:                        pulsar.Shared,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
			Name:                        serviceName,
		})

		if err != nil {
			errChan <- fmt.Errorf("error subscribing to topic. %v", err)
			return
		}

		defer consumer.Close()

		for {
			message, err := consumer.Recv(context.Background())
			if err == axon.ErrCloseConn {
				errChan <- axon.ErrCloseConn
				break
			}
			if err != nil {
				errChan <- err
				break
			}

			if message != nil {
				event := NewEvent(message, consumer)
				eventChan <- event
				break
			}
		}
	}(errChan, eventChan, req.GetReplyAddress())

	data, err := req.Compact()
	if err != nil {
		log.Printf("failed to compact request of: %s for transfer with the following errors: %v", topic, err)
		return err
	}

	if err := s.Publish(topic, data); err != nil {
		log.Printf("failed to send request for: %s with the following errors: %v", topic, err)
		return err
	}
	// Read address from
	for {
		select {
		case err, ok := <-errChan:
			if ok {
				return err
			}
		case event := <-eventChan:
			// This is the ReplyPayload
			var reply axon.ReplyPayload
			if err := json.Unmarshal(event.Data(), &reply); err != nil {
				event.Ack()
				return err
			}

			// Check if reply has an issue.
			if replyErr := reply.GetError(); replyErr != nil {
				event.Ack()
				return replyErr
			}

			// Unpack Reply's payload.
			if err := json.Unmarshal(reply.GetPayload(), v); err != nil {
				event.Ack()
				return err
			} else {
				event.Ack()
				return nil
			}
		}
	}
}

// Manually put the fqdn of your topics.
func (s *pulsarStore) Subscribe(topic string, handler axon.SubscriptionHandler) error {
	serviceName := s.GetServiceName()
	consumer, err := s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		AutoDiscoveryPeriod:         0,
		SubscriptionName:            fmt.Sprintf("%s-%s", serviceName, topic),
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		Name:                        serviceName,
	})
	if err != nil {
		return fmt.Errorf("error subscribing to topic. %v", err)
	}

	defer consumer.Close()
	for {
		message, err := consumer.Recv(context.Background())
		if err == axon.ErrCloseConn {
			break
		}
		if err != nil {
			continue
		}

		event := NewEvent(message, consumer)
		go handler(event)
	}
	return nil
}

// Note: If you need a more controlled init func, write your pulsar lib to implement the EventStore interface.
func Init(opts axon.Options) (axon.EventStore, error) {
	addr := strings.TrimSpace(opts.Address)
	if addr == "" {
		return nil, axon.ErrInvalidURL
	}

	name := strings.TrimSpace(opts.ServiceName)
	if name == "" {
		return nil, axon.ErrEmptyStoreName
	}

	clientOptions := pulsar.ClientOptions{URL: addr}
	if opts.CertContent != "" {
		certPath, err := initCert(opts.CertContent)
		if err != nil {
			return nil, err
		}
		clientOptions.TLSAllowInsecureConnection = true
		clientOptions.TLSTrustCertsFilePath = certPath
	}
	if opts.AuthenticationToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(opts.AuthenticationToken)
	}

	rand.Seed(time.Now().UnixNano())
	p, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to connect with Pulsar with provided configuration. failed with error: %v", err)
	}
	return &pulsarStore{client: newClientWrapper(p), serviceName: name}, nil
}

func InitTestEventStore(mockClient axon.Client, serviceName string) (axon.EventStore, error) {
	return &pulsarStore{client: mockClient, serviceName: serviceName}, nil
}

func (s *pulsarStore) GetServiceName() string {
	return s.serviceName
}

func (s *pulsarStore) Publish(topic string, message []byte) error {
	sn := s.GetServiceName()
	producer, err := s.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  fmt.Sprintf("%s-producer-%s", sn, generateRandomName()), // the servicename-producer-randomstring
	})
	if err != nil {
		return fmt.Errorf("failed to create new producer with the following error: %v", err)
	}
	// Always close producer after successful production of packets so as not to get the error of
	// ProducerBusy from pulsar.
	defer producer.Close()

	id, err := producer.Send(context.Background(), message)
	if err != nil {
		return fmt.Errorf("failed to send message. %v", err)
	}

	log.Printf("Published message to %s id ==>> %s", topic, byteToHex(id.Serialize()))
	return nil
}

func (s *pulsarStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func byteToHex(b []byte) string {
	var out struct{}
	_ = json.Unmarshal(b, &out)
	return hex.EncodeToString(b)
}

func generateRandomName() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 10)
	for i := range bytes {
		bytes[i] = chars[rand.Intn(len(chars))]
	}
	return string(bytes)
}

func initCert(content string) (string, error) {
	if len(content) == 0 {
		return "", errors.New("cert content is empty")
	}

	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	certPath := filepath.Join(pwd, "tls.crt")
	if err := ioutil.WriteFile(certPath, []byte(content), os.ModePerm); err != nil {
		return "", err
	}
	return certPath, nil
}
