package postgres

import (
	"context"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/sirupsen/logrus"
)

var logger *logrus.Entry

func Connect(ctx context.Context, connString string, log *logrus.Entry) (*pgx.Conn, error) {
	logger = log
	connConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	connConfig.Logger = logrusadapter.NewLogger(log)
	connConfig.LogLevel, err = pgx.LogLevelFromString(log.Logger.Level.String())
	if err != nil {
		return nil, err
	}
	// connConfig.PreferSimpleProtocol = true
	return pgx.ConnectConfig(ctx, connConfig)
}
