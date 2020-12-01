package postgres

import (
	"context"

	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

// Logger provides access to the PostgreSQL specific logging facility
var Logger *logrus.Entry

// DBExecutorContext interface represents sql executor with context support
type DBExecutorContext interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
}

// Connect function returns object that can execute sql against target database
var Connect func(ctx context.Context, connString string) (DBExecutorContext, error) = connect

func connect(ctx context.Context, connString string) (DBExecutorContext, error) {
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
	return pgxpool.ConnectConfig(ctx, connConfig)
}
