package postgres_test

import (
	"context"
	"testing"

	"github.com/cybertec-postgresql/debezium2postgres/internal/postgres"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	l := logrus.New().WithField("foo", "bar")
	_, err := postgres.Connect(context.Background(), "fooconnstr", l)
	assert.Error(t, err, "invalid dsn")

	l.Logger.Level = 42
	_, err = postgres.Connect(context.Background(), "postgres://user:password@host/db", l)
	assert.Error(t, err, "invalid log level")

	l.Logger.Level = logrus.InfoLevel
	postgres.NewConnecton = func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, nil
	}
	_, err = postgres.Connect(context.Background(), "postgres://user:password@host/db", l)
	assert.NoError(t, err)
}
