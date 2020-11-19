package main

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cybertec-postgresql/debezium2postgres/internal/cmdparser"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func getKafkaReader(brokers []string, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
}

func getTopics(brokers []string) ([]string, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	//get broker
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	defer cluster.Close()

	//get all topic from cluster
	return cluster.Topics()
}

func main() {
	cmdOpts, err := cmdparser.Parse()
	if err != nil {
		panic(err)
	}
	ll, err := logrus.ParseLevel(cmdOpts.LogLevel)
	if err != nil {
		ll = logrus.InfoLevel
	}
	var log = &logrus.Logger{
		Out: os.Stderr,
		Formatter: &logrus.TextFormatter{
			DisableLevelTruncation: true,
			DisableQuote:           true,
		},
		Level: ll,
	}
	log.WithField("options", cmdOpts).Debugln("Starting CDC migration...")
	//get topics
	topics, err := getTopics(cmdOpts.Kafka)
	if err != nil {
		log.Fatalln(err)
	}
	log.WithField("topics", topics).Printf("%d topics available", len(topics))

	//consume messages from topic
	var wg sync.WaitGroup
	for _, topic := range topics {
		log.WithField("topic", topic).WithField("prefix", cmdOpts.Topic).Debug("Checking for prefix")
		if strings.HasPrefix(topic, cmdOpts.Topic) {
			wg.Add(1)
			go func(topic string) {
				defer wg.Done()
				reader := getKafkaReader(cmdOpts.Kafka, topic)
				defer reader.Close()
				log.WithField("topic", topic).Println("Start consuming... !!")
				for {
					m, err := reader.ReadMessage(context.Background())
					if err != nil {
						log.Error(err)
						return
					}
					log.WithField("key", string(m.Key)).WithField("value", string(m.Value)).Info("Message consumed")
				}
			}(topic)
		}
	}
	wg.Wait()
}
