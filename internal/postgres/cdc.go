package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cybertec-postgresql/debezium2postgres/internal/kafka"
)

// Apply function reads messages from `messages` channel and applies changes to the target PostgreSQL database
func Apply(ctx context.Context, connString string, messages <-chan kafka.Message) {
	conn, err := Connect(context.Background(), connString)
	if err != nil {
		Logger.Fatalln(err)
		return
	}
	for {
		select {
		case m := <-messages:
			rowsAffected, err := applyCDCItem(ctx, conn, m)
			if err != nil {
				Logger.Error(err)
			} else if rowsAffected == 0 {
				Logger.Warning("CDC item caused no changes")
			}
		case <-ctx.Done():
			return
		}
	}
}

func applyCDCItem(ctx context.Context, conn DBExecutorContext, message kafka.Message) (int64, error) {
	Logger.WithField("schema", string(message.Key)).Trace("Key used for applying CDC item")
	switch message.Op {
	case "c":
		return insertCDCItem(ctx, conn, message)
	case "u":
		return updateCDCItem(ctx, conn, message)
	case "d":
		return deleteCDCItem(ctx, conn, message)
	case "r":
		// ignore snapshot reading
		return 0, nil
	}
	return 0, errors.New("Unsupported operation")
}

func insertCDCItem(ctx context.Context, conn DBExecutorContext, message kafka.Message) (int64, error) {
	l := Logger.WithField("op", "insert")
	l.Debug("Starting InsertCDCItem()...")
	fnumber := len(message.Values)
	refs := make([]string, 0, fnumber)
	for i := 1; i <= fnumber; i++ {
		refs = append(refs, "$"+strconv.Itoa(i))
	}
	args := make([]interface{}, 0, fnumber)
	fields := make([]string, len(args))
	for f, v := range message.Values {
		l.WithField("field", f).WithField("value", v).Debug("CDC value used")
		fields = append(fields, strconv.Quote(f))
		args = append(args, v)
	}
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)",
		message.QualifiedTablename(),
		strings.Join(fields, ","),
		strings.Join(refs, ","))
	ct, err := conn.Exec(ctx, sql, args...)
	l.Debug("Exiting InsertCDCItem()...")
	return ct.RowsAffected(), err
}

func updateCDCItem(ctx context.Context, conn DBExecutorContext, message kafka.Message) (int64, error) {
	l := Logger.WithField("op", "update")
	l.Debug("Starting UpdateCDCItem()...")
	keyrefs := make([]string, 0, len(message.Keys))
	for i := 1; i <= len(message.Keys); i++ {
		keyrefs = append(keyrefs, "$"+strconv.Itoa(i))
	}
	keyvals := make([]interface{}, 0, len(message.Keys))
	keyfields := make([]string, 0, len(message.Keys))
	for f, v := range message.Keys {
		keyfields = append(keyfields, strconv.Quote(f))
		keyvals = append(keyvals, v)
	}

	valrefs := make([]string, 0, len(message.Values))
	for i := 1; i <= len(message.Values); i++ {
		valrefs = append(valrefs, "$"+strconv.Itoa(i+len(message.Keys)))
	}
	vals := make([]interface{}, 0, len(message.Values))
	fields := make([]string, 0, len(message.Values))
	for f, v := range message.Values {
		fields = append(fields, strconv.Quote(f))
		vals = append(vals, v)
	}
	vals = append(keyvals, vals...)
	sql := fmt.Sprintf("UPDATE %s SET (%s)=(%s) WHERE (%s)=(%s)",
		message.QualifiedTablename(),
		strings.Join(fields, ","),
		strings.Join(valrefs, ","),
		strings.Join(keyfields, ","),
		strings.Join(keyrefs, ","))
	ct, err := conn.Exec(ctx, sql, vals...)
	l.Debug("Exiting UpdateCDCItem()...")
	return ct.RowsAffected(), err
}

func deleteCDCItem(ctx context.Context, conn DBExecutorContext, message kafka.Message) (int64, error) {
	l := Logger.WithField("op", "delete")
	l.Debug("Starting DeleteCDCItem()...")
	fnumber := len(message.Keys)
	refs := make([]string, 0, fnumber)
	for i := 1; i <= fnumber; i++ {
		refs = append(refs, "$"+strconv.Itoa(i))
	}
	args := make([]interface{}, 0, fnumber)
	fields := make([]string, 0, fnumber)
	for f, v := range message.Keys {
		l.WithField("field", f).WithField("oldvalue", v).Debug("CDC value used")
		fields = append(fields, strconv.Quote(f))
		args = append(args, v)
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE (%s)=(%s)",
		message.QualifiedTablename(),
		strings.Join(fields, ","),
		strings.Join(refs, ","))
	ct, err := conn.Exec(ctx, sql, args...)
	l.Debug("Exiting DeleteCDCItem()...")
	return ct.RowsAffected(), err
}
