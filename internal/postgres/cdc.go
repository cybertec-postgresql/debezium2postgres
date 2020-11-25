package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type CdcField struct {
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
	Field    string `json:"field"`
}

type CdcFields struct {
	Type     string     `json:"type"`
	Fields   []CdcField `json:"fields,omitempty"`
	Optional bool       `json:"optional"`
	Name     string     `json:"name,omitempty"`
	Field    string     `json:"field"`
}

type CdcSchema struct {
	Type     string      `json:"type"`
	Name     string      `json:"name"`
	Fields   []CdcFields `json:"fields"`
	Optional bool        `json:"optional"`
}

type CdcPayload struct {
	Before      *map[string]interface{} `json:"before"`
	After       *map[string]interface{} `json:"after"`
	Source      map[string]interface{}  `json:"source"`
	Op          string                  `json:"op"`
	Timestamp   int64                   `json:"ts_ms"`
	Transaction *map[string]interface{} `json:"transaction"`
}

type CdcMessage struct {
	Schema  *CdcSchema  `json:"schema"`
	Payload *CdcPayload `json:"payload"`
}

func ApplyCDCItem(ctx context.Context, conn DBExecutorContext, jsonData []byte) (int64, error) {
	var msg CdcMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		return -1, err
	}
	if msg.Payload == nil {
		return -1, errors.New("Payload is nil")
	}
	Logger.WithField("schema", msg.Schema).Trace("Schema used for applying CDC item")
	switch msg.Payload.Op {
	case "c":
		return InsertCDCItem(ctx, conn, msg.Payload)
	case "u":
		return UpdateCDCItem(ctx, conn, msg.Payload)
	case "d":
		return DeleteCDCItem(ctx, conn, msg.Payload)
	case "r":
		// ignore snapshot reading
		return 0, nil
	}
	return 0, errors.New("Unsupported operation")
}

func InsertCDCItem(ctx context.Context, conn DBExecutorContext, payload *CdcPayload) (int64, error) {
	l := Logger.WithField("op", "insert")
	if payload.After == nil {
		return -1, errors.New("Payload.After is nil")
	}
	l.Debug("Starting InsertCDCItem()...")
	fnumber := len(*payload.After)
	refs := make([]string, 0, fnumber)
	for i := 1; i <= fnumber; i++ {
		refs = append(refs, "$"+strconv.Itoa(i))
	}
	args := make([]interface{}, 0, fnumber)
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

func UpdateCDCItem(ctx context.Context, conn DBExecutorContext, payload *CdcPayload) (int64, error) {
	l := Logger.WithField("op", "update")
	if payload.Before == nil {
		return -1, errors.New("Payload.Before is nil")
	}
	if payload.After == nil {
		return -1, errors.New("Payload.After is nil")
	}
	l.Debug("Starting UpdateCDCItem()...")
	fnumber := len(*payload.After)
	oldrefs := make([]string, 0, fnumber)
	newrefs := make([]string, 0, fnumber)
	for i := 1; i <= fnumber; i++ {
		oldrefs = append(oldrefs, "$"+strconv.Itoa(i))
		newrefs = append(newrefs, "$"+strconv.Itoa(i+fnumber))
	}
	args := make([]interface{}, 0, fnumber)
	newargs := make([]interface{}, 0, fnumber)
	fields := make([]string, 0, fnumber)
	for f, v := range *payload.Before {
		l.WithField("field", f).WithField("oldvalue", v).Debug("CDC value used")
		fields = append(fields, strconv.Quote(f))
		args = append(args, v)
		v = (*payload.After)[f]
		l.WithField("field", f).WithField("newvalue", v).Debug("CDC value used")
		newargs = append(newargs, v)
	}
	args = append(args, newargs...)
	sql := fmt.Sprintf("UPDATE %s SET (%s)=(%s) WHERE (%s)=(%s)",
		payload.Source["table"],
		strings.Join(fields, ","),
		strings.Join(newrefs, ","),
		strings.Join(fields, ","),
		strings.Join(oldrefs, ","))
	ct, err := conn.Exec(ctx, sql, args...)
	l.Debug("Exiting UpdateCDCItem()...")
	return ct.RowsAffected(), err
}

func DeleteCDCItem(ctx context.Context, conn DBExecutorContext, payload *CdcPayload) (int64, error) {
	l := Logger.WithField("op", "delete")
	if payload.Before == nil {
		return -1, errors.New("Payload.Before is nil")
	}
	l.Debug("Starting DeleteCDCItem()...")
	fnumber := len(*payload.Before)
	refs := make([]string, 0, fnumber)
	for i := 1; i <= fnumber; i++ {
		refs = append(refs, "$"+strconv.Itoa(i))
	}
	args := make([]interface{}, 0, fnumber)
	fields := make([]string, 0, fnumber)
	for f, v := range *payload.Before {
		l.WithField("field", f).WithField("oldvalue", v).Debug("CDC value used")
		fields = append(fields, strconv.Quote(f))
		args = append(args, v)
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE (%s)=(%s)",
		payload.Source["table"],
		strings.Join(fields, ","),
		strings.Join(refs, ","))
	ct, err := conn.Exec(ctx, sql, args...)
	l.Debug("Exiting DeleteCDCItem()...")
	return ct.RowsAffected(), err
}
