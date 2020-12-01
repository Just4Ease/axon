package pulse

import (
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
	"sync"
	"time"
)

type pulsarStore struct {
	serviceName string
	client      axon.Client
	opts        axon.Options
}

func (s *pulsarStore) Reply(topic string, handler axon.ReplyHandler) error {

	// Execute Handler
	handlerPayload, handlerError := handler()
	replyPayload := axon.NewReply(handlerPayload, handlerError)
	data, err := replyPayload.Compact()
	if err != nil {
		return fmt.Errorf("unable to connect compact reply to be sent to topic with provided configuration. failed with error: %v", err)
	}

	return s.Publish(topic, data)
}

func (s *pulsarStore) Request(r string, message []byte, v interface{}) error {
	req := axon.NewRequestPayload(r, message)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error)
	eventChan := make(chan axon.Event)

	go func(wg *sync.WaitGroup, errChan chan error, eventChan chan axon.Event) {
		defer wg.Done()
		if err := s.Subscribe(req.GetReplyAddress(), func(event axon.Event) {
			eventChan <- event
			return
		}); err != nil {
			log.Printf("failed to receive response from: %s with the following errors: %v", r, err)
			errChan <- err
			return
		}
	}(wg, errChan, eventChan)

	data, err := req.Compact()
	if err != nil {
		log.Printf("failed to compact request of: %s for transfer with the following errors: %v", r, err)
		return err
	}

	if err := s.Publish(r, data); err != nil {
		log.Printf("failed to send request for: %s with the following errors: %v", r, err)
		return err
	}
	wg.Wait()
	// Read address from

	for {
		select {
		case err := <-errChan:
			return err

		case event := <-eventChan:
			// This is the ReplyPayload
			var reply axon.ReplyPayload
			if err := json.Unmarshal(event.Data(), &reply); err != nil {
				return err
			}

			// Check if reply has an issue.
			if replyErr := reply.GetError(); replyErr != nil {
				return replyErr
			}

			// Unpack Reply's payload.
			if err := json.Unmarshal(reply.GetPayload(), v); err != nil {
				return err
			}
		}
	}
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
