package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	pgx "github.com/jackc/pgx/v4"
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

type cdcPayload struct {
	Before      *map[string]interface{} `json:"before"`
	After       *map[string]interface{} `json:"after"`
	Source      map[string]interface{}  `json:"source"`
	Op          string                  `json:"op"`
	Timestamp   int64                   `json:"ts_ms"`
	Transaction *map[string]interface{} `json:"transaction"`
}

type cdcMessage struct {
	Schema  *cdcSchema  `json:"schema"`
	Payload *cdcPayload `json:"payload"`
}

func ApplyCDCItem(ctx context.Context, conn *pgx.Conn, jsonData []byte) (int64, error) {
	var msg cdcMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		return -1, err
	}
	logger.WithField("schema", msg.Schema).Trace("Schema used for applying CDC item")
	switch msg.Payload.Op {
	case "c":
		return InsertCDCItem(ctx, conn, msg.Payload)
	case "u":
		return UpdateCDCItem(ctx, conn, msg.Payload)
	case "d":
		return DeleteCDCItem(ctx, conn, msg.Payload)
	case "r":
		// ignore snapshot reading
	}
	return 0, nil
}

func InsertCDCItem(ctx context.Context, conn *pgx.Conn, payload *cdcPayload) (int64, error) {
	l := logger.WithField("op", "insert")
	l.Debug("Starting InsertCDCItem()...")
	refs := make([]string, 0, len(*payload.After))
	for i := 1; i <= len(*payload.After); i++ {
		refs = append(refs, "$"+strconv.Itoa(i))
	}
	args := make([]interface{}, 0, len(*payload.After))
	fields := make([]string, len(args))
	for f, v := range *payload.After {
		l.WithField("field", f).WithField("value", v).Debug("CDC value used")
		fields = append(fields, strconv.Quote(f))
		args = append(args, v)
	}
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)",
		payload.Source["table"],
		strings.Join(fields, ","),
		strings.Join(refs, ","))
	ct, err := conn.Exec(ctx, sql, args...)
	l.Debug("Exiting InsertCDCItem()...")
	return ct.RowsAffected(), err
}

func UpdateCDCItem(ctx context.Context, conn *pgx.Conn, payload *cdcPayload) (int64, error) {
	// ct, err := conn.Exec(ctx, "INSERT INTO tablename VALUES (fields)")
	// return ct.RowsAffected(), err
	return 0, nil
}

func DeleteCDCItem(ctx context.Context, conn *pgx.Conn, payload *cdcPayload) (int64, error) {
	// ct, err := conn.Exec(ctx, "INSERT INTO tablename VALUES (fields)")
	// return ct.RowsAffected(), err
	return 0, nil
}
