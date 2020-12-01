package postgres

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestConnect")
	_, err := connect(context.Background(), "fooconnstr")
	assert.Error(t, err, "invalid dsn")

	Logger.Logger.Level = 42
	_, err = connect(context.Background(), "postgres://user:password@host/db")
	assert.Error(t, err, "invalid log level")

	Logger.Logger.Level = logrus.InfoLevel
	_, err = connect(context.Background(), "postgres://user:password@host/db")
	assert.Error(t, err, "host resolving error")
}
