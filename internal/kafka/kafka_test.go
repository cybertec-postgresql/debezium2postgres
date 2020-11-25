package kafka_test

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
	"github.com/stretchr/testify/assert"
)

func TestGetReader(t *testing.T) {
	// return kafkago.NewReader(kafka.ReaderConfig{
	// 	Brokers:   brokers,
	// 	Topic:     topic,
	// 	Partition: 0,
	// 	MinBytes:  10e3, // 10KB
	// 	MaxBytes:  10e6, // 10MB
	// })
}

func TestGetTopics(t *testing.T) {
	kafka.NewConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		return nil, errors.New("Failed")
	}
	_, err := kafka.GetTopics([]string{"foo", "bar"})
	assert.Error(t, err)

	kafka.NewConsumer = func(addrs []string, config *sarama.Config) (sarama.Consumer, error) {
		return mocks.NewConsumer(t, nil), nil
	}
	_, err = kafka.GetTopics([]string{"foo", "bar"})
	assert.Error(t, err)
}
