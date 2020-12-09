package kafka

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	m := kafka.Message{
		Value: []byte(`{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"first_name"},{"type":"string","optional":false,"field":"last_name"},{"type":"string","optional":false,"field":"email"},{"type":"int64","optional":true,"field":"__source_ts_ms"},{"type":"string","optional":true,"field":"__db"},{"type":"string","optional":true,"field":"__table"},{"type":"string","optional":true,"field":"__op"},{"type":"string","optional":true,"field":"__deleted"}],"optional":false,"name":"dbserver1.inventory.customers.Value"},"payload":{"id":1003,"first_name":"Edward","last_name":"Walker","email":"ed@walker.com","__source_ts_ms":0,"__db":"inventory","__schema":"public","__table":"customers","__op":"c","__deleted":"false"}}`),
		Key:   []byte(`{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"}],"optional":false,"name":"dbserver1.inventory.customers.Key"},"payload":{"id":1003}}`),
	}
	msg, err := NewMessage(m)
	assert.NoError(t, err)
	assert.NotNil(t, msg)

	m.Value = []byte(`{"schema":null, "payload":null}`)
	msg, err = NewMessage(m)
	assert.Error(t, err, "Corrupted value payload")
	assert.Nil(t, msg)

	m.Value = []byte{}
	msg, err = NewMessage(m)
	assert.Error(t, err, "Corrupted value")
	assert.Nil(t, msg)

	m.Key = []byte(`{"schema":null, "payload":null}`)
	msg, err = NewMessage(m)
	assert.Error(t, err, "Corrupted key payload")
	assert.Nil(t, msg)

	m.Key = []byte{}
	msg, err = NewMessage(m)
	assert.Error(t, err, "Corrupted key")
	assert.Nil(t, msg)
}

func TestQualifiedTableName(t *testing.T) {
	m := Message{}
	m.TableName = "bar"
	assert.Equal(t, m.QualifiedTablename(), `"bar"`, "No schema used")
	m.SchemaName = "foo"
	assert.Equal(t, m.QualifiedTablename(), `"foo"."bar"`, "Schema qualified")
}
