package main

import (
	"context"
	"fmt"
	"os"

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
		Formatter: &logrus.JSONFormatter{
			PrettyPrint: false, //set to true to debug
		},
		Level: ll,
	}
	log.WithField("cmdoptions", cmdOpts).Debugln("Starting CDC migration...")
	//get topics
	topics, err := getTopics(cmdOpts.Kafka)
	if err != nil {
		log.Fatalln(err)
	}
	log.WithField("topics", topics).Printf("%d topics available", len(topics))

	//consume messages from topic
	reader := getKafkaReader(cmdOpts.Kafka, cmdOpts.Topic)
	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("\nmessage at topic:%v partition:%v offset:%v\n\t%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
