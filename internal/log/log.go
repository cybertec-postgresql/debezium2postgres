package log

import (
	"os"

	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
	"github.com/cybertec-postgresql/debezium2postgres/internal/postgres"
	"github.com/sirupsen/logrus"
)

func Init(level string) *logrus.Logger {
	ll, err := logrus.ParseLevel(level)
	if err != nil {
		ll = logrus.InfoLevel
	}
	var log = &logrus.Logger{
		Out: os.Stderr,
		Formatter: &logrus.TextFormatter{
			DisableLevelTruncation: true,
			DisableQuote:           true,
			PadLevelText:           true,
		},
		Level: ll,
	}
	//init loggers
	kafka.Logger = log.WithField("module", "kafka")
	postgres.Logger = log.WithField("module", "postgres")
	return log
}
