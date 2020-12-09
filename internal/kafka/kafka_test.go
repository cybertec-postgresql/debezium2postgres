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
	if ctx.Err() != nil {
		return kafka.Message{}, ctx.Err()
	}
	if r.ReadMessageHandler != nil {
		return r.ReadMessageHandler()
	}
	time.Sleep(500 * time.Millisecond)
	return kafka.Message{
		Value: []byte(`{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"first_name"},{"type":"string","optional":false,"field":"last_name"},{"type":"string","optional":false,"field":"email"},{"type":"int64","optional":true,"field":"__source_ts_ms"},{"type":"string","optional":true,"field":"__db"},{"type":"string","optional":true,"field":"__table"},{"type":"string","optional":true,"field":"__op"},{"type":"string","optional":true,"field":"__deleted"}],"optional":false,"name":"dbserver1.inventory.customers.Value"},"payload":{"id":1003,"first_name":"Edward","last_name":"Walker","email":"ed@walker.com","__source_ts_ms":0,"__db":"inventory","__table":"customers","__op":"c","__deleted":"false"}}`),
		Key:   []byte(`{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"}],"optional":false,"name":"dbserver1.inventory.customers.Key"},"payload":{"id":1003}}`),
	}, nil
}

func (r *mockKafkaReader) Close() error {
	return nil
}
func TestConsumeTopic(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestConsumeTopic")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	getReader = func(brokers []string, topic string) kafkaReader {
		return &mockKafkaReader{}
	}
	consumeTopic(ctx, []string{"foo", "bar"}, "baz", make(chan Message, 10))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	getReader = func(brokers []string, topic string) kafkaReader {
		return &mockKafkaReader{
			ReadMessageHandler: func() (kafka.Message, error) {
				time.Sleep(500 * time.Millisecond)
				return kafka.Message{}, nil
			}}
	}
	consumeTopic(ctx, []string{"foo", "bar"}, "baz", make(chan Message, 10))
}
