package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Logger provides access to the Kafka specific logging facility
var Logger *logrus.Entry

type kafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// getReader returns a kafka reader to consume messages from `brokers` with `topic`
var getReader = func(brokers []string, topic string) kafkaReader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
}

var newConsumer = sarama.NewConsumer

func getTopics(brokers []string) ([]string, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	//get broker
	cluster, err := newConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	defer cluster.Close()
	//get all topic from cluster
	topics, err := cluster.Topics()
	Logger.WithField("topics", topics).Debug("kafka topics returned")
	return topics, err
}

// Consume function receives messages from Kafka and sends them to the `messages` channel
func Consume(ctx context.Context, brokers []string, topicPattern string, messages chan<- []byte) {
	Logger.Debug("Starting consuming from kafka...")
	topics, err := getTopics(brokers)
	if err != nil {
		Logger.Fatalln(err)
	}
	for _, topic := range topics {
		Logger.WithField("topic", topic).WithField("prefix", topicPattern).Debug("Checking for prefix")
		if strings.HasPrefix(topic, topicPattern) {
			go consumeTopic(context.Background(), brokers, topic, messages)
		}
	}
}
func consumeTopic(ctx context.Context, brokers []string, topic string, messages chan<- []byte) {
	topiclogger := Logger.WithField("topic", topic)
	reader := getReader(brokers, topic)
	defer reader.Close()
	topiclogger.Println("Starting consuming topic...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			topiclogger.Error(err)
			return
		}
		topiclogger.WithField("key", string(m.Key)).WithField("value", string(m.Value)).Trace("Message consumed")
		messages <- m.Value
	}
}
