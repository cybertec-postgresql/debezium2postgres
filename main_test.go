package main

import (
	"os"
	"testing"

	"github.com/cybertec-postgresql/debezium2postgres/internal/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	osExit = func(code int) {
		t.Logf("os.Exit(%d) called", code)
		panic(code)
	}
	assert.Panics(t, func() { main() })

	initLog = func(level string) *logrus.Logger {
		l := log.Init(level)
		l.ExitFunc = func(code int) {
			t.Logf("os.Exit(%d) called", code)
		}
		return l
	}
	os.Args = []string{0: "go-test", "--kafka=connstr", "--topic=foo", "--loglevel=trace", "--postgres=connstr"}
	assert.NotPanics(t, func() { main() })
}
