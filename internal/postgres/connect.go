package postgres

import (
	"context"

	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

var Logger *logrus.Entry

type DBExecutorContext interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
}

var NewConnecton = pgxpool.ConnectConfig

func Connect(ctx context.Context, connString string) (DBExecutorContext, error) {
	connConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	connConfig.ConnConfig.Logger = logrusadapter.NewLogger(Logger)
	connConfig.ConnConfig.LogLevel, err = pgx.LogLevelFromString(Logger.Logger.Level.String())
	if err != nil {
		return nil, err
	}
	// connConfig.PreferSimpleProtocol = true
	return NewConnecton(ctx, connConfig)
}
