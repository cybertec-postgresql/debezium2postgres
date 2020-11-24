package main

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/cybertec-postgresql/debezium2postgres/internal/cmdparser"
	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
	"github.com/cybertec-postgresql/debezium2postgres/internal/postgres"
	"github.com/sirupsen/logrus"
)

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
	kafkalogger := log.WithField("module", "kafka")
	topics, err := kafka.GetTopics(cmdOpts.Kafka)
	if err != nil {
		log.Fatalln(err)
	}
	kafkalogger.WithField("topics", topics).Debug("kafka topics returned")
	kafkalogger.Printf("kafka topics returned: %d", len(topics))

	pglogger := log.WithField("module", "postgres")
	db, err := postgres.Connect(context.Background(), cmdOpts.Postgres, pglogger)
	if err != nil {
		log.Fatalln(err)
	}
	pglogger.WithField("connstring", db.Config().ConnString()).Debug("PostgreSQL connection established")
	pglogger.Println("PostgreSQL connection established")
	_, _ = db.Exec(context.Background(), "SELECT version()")

	//consume messages from topic
	var wg sync.WaitGroup
	for _, topic := range topics {
		kafkalogger.WithField("topic", topic).WithField("prefix", cmdOpts.Topic).Debug("Checking for prefix")
		if strings.HasPrefix(topic, cmdOpts.Topic) {
			wg.Add(1)
			go func(topic string) {
				topiclogger := kafkalogger.WithField("topic", topic)
				defer wg.Done()
				reader := kafka.GetReader(cmdOpts.Kafka, topic)
				defer reader.Close()
				topiclogger.Println("Start consuming...")
				for {
					m, err := reader.ReadMessage(context.Background())
					if err != nil {
						topiclogger.Error(err)
						return
					}
					topiclogger.WithField("key", string(m.Key)).WithField("value", string(m.Value)).Debug("Message consumed")
					_, err = postgres.ApplyCDCItem(context.Background(), db, m.Value)
					if err != nil {
						topiclogger.Error(err)
					}
				}
			}(topic)
		}
	}
	wg.Wait()
}
