package cmdparser

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFail(t *testing.T) {
	os.Args = []string{"go-test", "--foo"}
	_, err := Parse()
	assert.Error(t, err, "Uknown option --foo")

	os.Args = []string{"go-test", "--topic=required"}
	_, err = Parse()
	assert.NoError(t, err, "Required options specified")
}
