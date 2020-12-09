package kafka

import (
	"encoding/json"
	"errors"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

type cdcField struct {
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
	Field    string `json:"field"`
}

type cdcFields struct {
	Type     string     `json:"type"`
	Fields   []cdcField `json:"fields,omitempty"`
	Optional bool       `json:"optional"`
	Name     string     `json:"name,omitempty"`
	Field    string     `json:"field"`
}

type cdcSchema struct {
	Type     string      `json:"type"`
	Name     string      `json:"name"`
	Fields   []cdcFields `json:"fields"`
	Optional bool        `json:"optional"`
}

type cdcMessage struct {
	Schema  *cdcSchema              `json:"schema"`
	Payload *map[string]interface{} `json:"payload"`
}

type cdcKey struct {
	Schema  *cdcSchema              `json:"schema"`
	Payload *map[string]interface{} `json:"payload"`
}

// Message is a data structure representing kafka messages
type Message struct {
	kafka.Message
	Op         string
	TableName  string
	SchemaName string
	Keys       map[string]interface{}
	Values     map[string]interface{}
}

// NewMessage used to create and init a new message instance
func NewMessage(msg kafka.Message) (*Message, error) {
	var err error
	message := &Message{
		Message: msg,
		Keys:    make(map[string]interface{}),
		Values:  make(map[string]interface{}),
	}
	err = message.initKeys()
	if err != nil {
		return nil, err
	}
	err = message.initValues()
	if err != nil {
		return nil, err
	}
	return message, nil
}

// initKeys inits keys with the values to use in SQL DML statement
func (m *Message) initKeys() error {
	var key cdcKey
	if err := json.Unmarshal(m.Key, &key); err != nil {
		return err
	}
	if key.Payload == nil {
		return errors.New("Payload is nil")
	}
	m.Keys = *key.Payload
	return nil
}

// initValues inits table name, operation and field names with the values to use in SQL DML statement
func (m *Message) initValues() error {
	var msg cdcMessage
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		return err
	}
	if msg.Payload == nil {
		return errors.New("Payload is nil")
	}
	for k, v := range *msg.Payload {
		if strings.HasPrefix(k, "__") { // system fields
			switch k {
			case "__schema":
				m.SchemaName = v.(string)
			case "__table":
				m.TableName = v.(string)
			case "__op":
				m.Op = v.(string)
			}
			continue
		}
		m.Values[k] = v
	}
	return nil
}

func (m *Message) QualifiedTablename() string {
	quoteIdent := func(s string) string {
		return `"` + strings.Replace(s, `"`, `""`, -1) + `"`
	}
	if m.SchemaName > "" {
		return quoteIdent(m.SchemaName) + "." + quoteIdent(m.TableName)
	}
	return quoteIdent(m.TableName)
}
