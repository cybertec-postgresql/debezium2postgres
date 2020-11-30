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
	postgres.Logger = logrus.New().WithField("foo", "bar")
	_, err := postgres.Connect(context.Background(), "fooconnstr")
	assert.Error(t, err, "invalid dsn")

	postgres.Logger.Logger.Level = 42
	_, err = postgres.Connect(context.Background(), "postgres://user:password@host/db")
	assert.Error(t, err, "invalid log level")

	postgres.Logger.Logger.Level = logrus.InfoLevel
	postgres.NewConnecton = func(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, nil
	}
	_, err = postgres.Connect(context.Background(), "postgres://user:password@host/db")
	assert.NoError(t, err)
}
