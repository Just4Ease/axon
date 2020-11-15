package pulse

import (
	"axon"
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

type producerWrapper struct {
	producer pulsar.Producer
}

func (p *producerWrapper) Send(ctx context.Context, data []byte) (pulsar.MessageID, error) {
	return p.producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: data, EventTime: time.Now(),
	})
}

func (p *producerWrapper) Close() {
	p.producer.Close()
}

type consumerWrapper struct {
	consumer pulsar.Consumer
}

func (c *consumerWrapper) Recv(ctx context.Context) (axon.Message, error) {
	return c.consumer.Receive(ctx)
}

func (c *consumerWrapper) Ack(id pulsar.MessageID) {
	c.consumer.AckID(id)
}

func (c *consumerWrapper) Close() {
	c.consumer.Close()
}

type clientWrapper struct {
	client pulsar.Client
}

func newClientWrapper(p pulsar.Client) axon.Client {
	return &clientWrapper{client: p}
}

func (c *clientWrapper) CreateProducer(opt pulsar.ProducerOptions) (axon.Producer, error) {
	p, err := c.client.CreateProducer(opt)
	if err != nil {
		return nil, err
	}
	return &producerWrapper{producer: p}, nil
}

func (c *clientWrapper) Subscribe(opt pulsar.ConsumerOptions) (axon.Consumer, error) {
	consumer, err := c.client.Subscribe(opt)
	if err != nil {
		return nil, err
	}
	return &consumerWrapper{consumer: consumer}, nil
}

func (c *clientWrapper) CreateReader(opt pulsar.ReaderOptions) (pulsar.Reader, error) {
	return c.client.CreateReader(opt)
}

func (c *clientWrapper) TopicPartitions(topic string) ([]string, error) {
	return c.client.TopicPartitions(topic)
}

func (c *clientWrapper) Close() {
	c.client.Close()
}
