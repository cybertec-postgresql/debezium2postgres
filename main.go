package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
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
	kafkaURL := "10.0.0.105:9092"

	//get topics
	topics, err := getTopics([]string{kafkaURL})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%d topics available:\n", len(topics))
	for index := range topics {
		fmt.Println("\t" + topics[index])
	}

	//consume messages from topic
	topic := "dbserver1"

	reader := getKafkaReader(kafkaURL, topic)
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
