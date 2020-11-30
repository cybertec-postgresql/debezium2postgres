package main

import (
	"context"

	"github.com/cybertec-postgresql/debezium2postgres/internal/cmdparser"
	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
	"github.com/cybertec-postgresql/debezium2postgres/internal/log"
	"github.com/cybertec-postgresql/debezium2postgres/internal/postgres"
)

func main() {
	cmdOpts, err := cmdparser.Parse()
	if err != nil {
		panic(err)
	}
	log := log.Init(cmdOpts.LogLevel)
	log.WithField("options", cmdOpts).Debug("Starting CDC migration...")

	db, err := postgres.Connect(context.Background(), cmdOpts.Postgres)
	if err != nil {
		log.Fatalln(err)
	}

	// create channel for passing messages to database worker
	var msgChannel chan []byte = make(chan []byte, 16)
	kafka.Consume(context.Background(), cmdOpts.Kafka, cmdOpts.Topic, msgChannel)
	postgres.Apply(context.Background(), db, msgChannel)
}
