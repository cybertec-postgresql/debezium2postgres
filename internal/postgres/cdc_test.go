package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type MockDbExec struct {
	DBExecutorContext
	ExecHandler func() (pgconn.CommandTag, error)
}

func (m MockDbExec) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	if m.ExecHandler != nil {
		return m.ExecHandler()
	}
	return nil, nil
}

func TestApply(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestApply")
	Logger.Logger.ExitFunc = func(int) {
		t.Log("log.Fatal called")
	}

	var msgChan chan []byte = make(chan []byte, 2)
	msgChan <- []byte(`foo`)
	msgChan <- []byte(`{
  "schema": null,
  "payload": {
    "before": {
      "id": 16
    },
    "after": null,
    "source": null,
    "op": "d"
  }
}`)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	Connect = func(ctx context.Context, connString string) (DBExecutorContext, error) {
		return nil, errors.New("bad connection")
	}
	Apply(ctx, "foo", msgChan)

	Connect = func(ctx context.Context, connString string) (DBExecutorContext, error) {
		return &MockDbExec{
			ExecHandler: func() (pgconn.CommandTag, error) {
				return pgconn.CommandTag("no affected rows"), nil
			},
		}, nil
	}
	Apply(ctx, "foo", msgChan)
}

func TestApplyCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestApplyCDCItem")

	_, err := applyCDCItem(context.Background(), MockDbExec{}, []byte(`foo`))
	assert.Error(t, err, "Invalid JSON")

	_, err = applyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": null}`))
	assert.Error(t, err, "Payload is nil")

	_, err = applyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "foo"}}`))
	assert.Error(t, err, "Unsupported operation")

	_, err = applyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "c"}}`))
	assert.Error(t, err, "Payload.After is nil")

	_, err = applyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "u"}}`))
	assert.Error(t, err, "Payload.After is nil")

	_, err = applyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "d"}}`))
	assert.Error(t, err, "Payload.After is nil")

	res, err := applyCDCItem(context.Background(), MockDbExec{}, []byte(`{"payload": {"op": "r"}}`))
	assert.NoError(t, err, "Payload.After is nil")
	assert.Equal(t, int64(0), res, "ignore snapshot reading")
}

func TestInsertCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestInsertCDCItem")

	_, err := insertCDCItem(context.Background(), MockDbExec{}, &cdcPayload{})
	assert.Error(t, err, "Payload.After is nil")

	_, err = insertCDCItem(context.Background(), MockDbExec{},
		&cdcPayload{After: &map[string]interface{}{"field1": "value1", "field2": "value2"}})
	assert.NoError(t, err)
}

func TestUpdateCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestUpdateCDCItem")

	_, err := updateCDCItem(context.Background(), MockDbExec{}, &cdcPayload{})
	assert.Error(t, err, "Payload.Before is nil")

	_, err = updateCDCItem(context.Background(), MockDbExec{},
		&cdcPayload{Before: &map[string]interface{}{"field1": "value1", "field2": "value2"}})
	assert.Error(t, err, "Payload.After is nil")

	_, err = updateCDCItem(context.Background(), MockDbExec{},
		&cdcPayload{
			After:  &map[string]interface{}{"field1": "value1", "field2": "value2"},
			Before: &map[string]interface{}{"field1": "value1", "field2": "value2"},
		})
	assert.NoError(t, err)
}

func TestDeleteCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestDeleteCDCItem")

	_, err := deleteCDCItem(context.Background(), MockDbExec{}, &cdcPayload{})
	assert.Error(t, err, "Payload.Before is nil")

	_, err = deleteCDCItem(context.Background(), MockDbExec{},
		&cdcPayload{Before: &map[string]interface{}{"field1": "value1", "field2": "value2"}})
	assert.NoError(t, err)
}
