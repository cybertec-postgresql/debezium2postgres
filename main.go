package main

import (
	"context"
	"os"

	"github.com/cybertec-postgresql/debezium2postgres/internal/cmdparser"
	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
	"github.com/cybertec-postgresql/debezium2postgres/internal/log"
	"github.com/cybertec-postgresql/debezium2postgres/internal/postgres"
)

var osExit = os.Exit
var initLog = log.Init

func main() {
	cmdOpts, err := cmdparser.Parse()
	if err != nil {
		osExit(2)
	}
	log := initLog(cmdOpts.LogLevel)
	log.WithField("options", cmdOpts).Debug("Starting CDC migration...")

	// create channel for passing messages to database worker
	var msgChannel chan kafka.Message = make(chan kafka.Message, 16)
	kafka.Consume(context.Background(), cmdOpts.Kafka, cmdOpts.Topic, msgChannel)
	postgres.Apply(context.Background(), cmdOpts.Postgres, msgChannel)
}
