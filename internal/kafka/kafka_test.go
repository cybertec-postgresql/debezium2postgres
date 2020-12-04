package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGetReader(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestGetReader")
	assert.NotNil(t, getReader([]string{"foo", "bar"}, "baz"))
}

func TestGetTopics(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestGetTopics")
	newConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		return nil, errors.New("Failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := getTopics([]string{"foo", "bar"})
	assert.Error(t, err)
	Logger.Logger.ExitFunc = func(int) {
		t.Log("log.Fatal called")
	}
	Consume(ctx, []string{"foo", "bar"}, "baz", make(chan Message, 1))

	newConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		c := mocks.NewConsumer(t, nil)
		c.SetTopicMetadata(map[string][]int32{"foo": {1, 2, 3}})
		return c, nil
	}
	topics, err := getTopics([]string{"foo", "bar"})
	Consume(ctx, []string{"foo", "bar"}, "foo", make(chan Message, 1))
	assert.NoError(t, err)
	assert.Equal(t, topics, []string{"foo"})
}

type mockKafkaReader struct {
	kafkaReader
	ReadMessageHandler func() (kafka.Message, error)
}

func (r *mockKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if r.ReadMessageHandler != nil {
		return r.ReadMessageHandler()
	}
	if ctx.Err() != nil {
		return kafka.Message{}, ctx.Err()
	}
	time.Sleep(500 * time.Millisecond)
	return kafka.Message{}, nil
}

func (r *mockKafkaReader) Close() error {
	return nil
}
func TestConsumeTopic(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestConsumeTopic")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	consumeTopic(ctx, []string{"foo", "bar"}, "baz", make(chan Message, 10))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	getReader = func(brokers []string, topic string) kafkaReader {
		return &mockKafkaReader{}
	}
	consumeTopic(ctx, []string{"foo", "bar"}, "baz", make(chan Message, 10))
}
