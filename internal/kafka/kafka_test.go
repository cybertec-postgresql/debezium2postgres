package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
	"github.com/cybertec-postgresql/debezium2postgres/internal/log"
	"github.com/stretchr/testify/assert"
)

func TestGetReader(t *testing.T) {
	assert.NotNil(t, kafka.GetReader([]string{"foo", "bar"}, "baz"))
}

func TestGetTopics(t *testing.T) {
	_ = log.Init(map[bool]string{false: "info", true: "debug"}[testing.Verbose()])
	kafka.NewConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		return nil, errors.New("Failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	_, err := kafka.GetTopics([]string{"foo", "bar"})
	assert.Error(t, err)
	kafka.Logger.Logger.ExitFunc = func(int) {
		t.Log("log.Fatal called")
	}
	kafka.Consume(ctx, []string{"foo", "bar"}, "baz", make(chan []byte, 1))

	kafka.NewConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		c := mocks.NewConsumer(t, nil)
		c.SetTopicMetadata(map[string][]int32{"foo": {1, 2, 3}})
		return c, nil
	}
	topics, err := kafka.GetTopics([]string{"foo", "bar"})
	kafka.Consume(ctx, []string{"foo", "bar"}, "foo", make(chan []byte, 1))
	assert.NoError(t, err)
	assert.Equal(t, topics, []string{"foo"})
}
