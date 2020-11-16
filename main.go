package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

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

func main() {
	// to consume messages
	topic := "dbserver1.inventory.customers"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "10.0.1.112:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}

	// get kafka reader using environment variables.
	// kafkaURL := "10.0.1.112:9092"
	// topic := "dbserver1.inventory.customers"

	// reader := getKafkaReader(kafkaURL, topic)

	// defer reader.Close()

	// fmt.Println(reader.Stats())
	// fmt.Println("start consuming ... !!")
	// for {
	// 	m, err := reader.ReadMessage(context.Background())
	// 	if err != nil {
	// 		log.Fatalln(err)
	// 	}
	// 	fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	// }
}
