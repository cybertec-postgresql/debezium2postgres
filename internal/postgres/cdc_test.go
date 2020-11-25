package postgres_test

import (
	"context"
	"testing"

	"github.com/cybertec-postgresql/debezium2postgres/internal/postgres"
	"github.com/jackc/pgconn"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type MockDbExec struct {
	postgres.DBExecutorContext
}

func (m MockDbExec) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return nil, nil
}

func TestApplyCDCItem(t *testing.T) {
	postgres.Logger = logrus.New().WithField("method", "TestApplyCDCItem")

	_, err := postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`foo`))
	assert.Error(t, err, "Invalid JSON")

	_, err = postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": null}`))
	assert.Error(t, err, "Payload is nil")

	_, err = postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "foo"}}`))
	assert.Error(t, err, "Unsupported operation")

	_, err = postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "c"}}`))
	assert.Error(t, err, "Payload.After is nil")

	_, err = postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "u"}}`))
	assert.Error(t, err, "Payload.After is nil")

	_, err = postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "d"}}`))
	assert.Error(t, err, "Payload.After is nil")

	res, err := postgres.ApplyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "r"}}`))
	assert.NoError(t, err, "Payload.After is nil")
	assert.Equal(t, int64(0), res, "ignore snapshot reading")
}

func TestInsertCDCItem(t *testing.T) {
	postgres.Logger = logrus.New().WithField("method", "TestInsertCDCItem")
	postgres.Logger.Level = logrus.TraceLevel

	_, err := postgres.InsertCDCItem(context.Background(), MockDbExec{},
		&postgres.CdcPayload{After: &map[string]interface{}{"field1": "value1", "field2": "value2"}})
	assert.NoError(t, err)
}
