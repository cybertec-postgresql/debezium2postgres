package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
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

	var msgChan chan kafka.Message = make(chan kafka.Message, 2)
	msg := kafka.Message{}
	msgChan <- msg
	msg.Op = "c"
	msgChan <- msg

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	Connect = func(ctx context.Context, connString string) (DBExecutorContext, error) {
		return nil, errors.New("bad connection")
	}
	Apply(ctx, "foo", time.Second, msgChan)

	Connect = func(ctx context.Context, connString string) (DBExecutorContext, error) {
		return &MockDbExec{
			ExecHandler: func() (pgconn.CommandTag, error) {
				return pgconn.CommandTag("no affected rows"), nil
			},
		}, nil
	}
	Apply(ctx, "foo", time.Second, msgChan)
}

func TestApplyCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestApplyCDCItem")

	msg := kafka.Message{}
	_, err := applyCDCItem(context.Background(), MockDbExec{}, msg)
	assert.Error(t, err, "Invalid JSON")

	msg.Op = "foo"
	_, err = applyCDCItem(context.Background(), MockDbExec{}, msg)
	assert.Error(t, err, "Unsupported operation")

	msg.Op = "c"
	_, err = applyCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)

	msg.Op = "u"
	_, err = applyCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)

	msg.Op = "d"
	_, err = applyCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)

	msg.Op = "r"
	res, err := applyCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res, "ignore snapshot reading")
}

func TestInsertCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestInsertCDCItem")
	msg := kafka.Message{
		Keys:   make(map[string]interface{}),
		Values: make(map[string]interface{}),
	}
	msg.Keys["foo"] = "bar"
	msg.Values["foo"] = "baz"
	_, err := insertCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)
}

func TestUpdateCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestUpdateCDCItem")
	msg := kafka.Message{
		Keys:   make(map[string]interface{}),
		Values: make(map[string]interface{}),
	}
	msg.Keys["foo"] = "bar"
	msg.Values["foo"] = "baz"
	_, err := updateCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)
}

func TestDeleteCDCItem(t *testing.T) {
	Logger = logrus.New().WithField("method", "TestDeleteCDCItem")
	msg := kafka.Message{
		Keys:   make(map[string]interface{}),
		Values: make(map[string]interface{}),
	}
	msg.Keys["foo"] = "bar"
	msg.Values["foo"] = "baz"
	_, err := deleteCDCItem(context.Background(), MockDbExec{}, msg)
	assert.NoError(t, err)
}
