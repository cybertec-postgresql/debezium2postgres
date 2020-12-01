package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
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
	Consume(ctx, []string{"foo", "bar"}, "baz", make(chan []byte, 1))

	newConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		c := mocks.NewConsumer(t, nil)
		c.SetTopicMetadata(map[string][]int32{"foo": {1, 2, 3}})
		return c, nil
	}
	topics, err := getTopics([]string{"foo", "bar"})
	Consume(ctx, []string{"foo", "bar"}, "foo", make(chan []byte, 1))
	assert.NoError(t, err)
	assert.Equal(t, topics, []string{"foo"})
}
