package cmdparser

import (
	"os"

	flags "github.com/jessevdk/go-flags"
)

// CmdOptions holds command line options passed
type CmdOptions struct {
	LogLevel string   `long:"loglevel" default:"info" description:"Set logging vefrobisty level, e.g. info, error, debug, trace" env:"DBZ2PG_LOGLEVEL"`
	Postgres string   `long:"postgres" description:"PostgreSQL connection string" env:"DBZ2PG_PGURL"`
	Kafka    []string `long:"kafka" description:"Kafka connection string" env:"DBZ2PG_KAFKA"`
	Topic    string   `long:"topic" description:"Topic name (or prefix of the topic name) to consume" env:"DBZ2PG_TOPIC" required:"True"`
}

// Parse will parse command line arguments and initialize pgengine
func Parse() (*CmdOptions, error) {
	cmdOpts := new(CmdOptions)
	parser := flags.NewParser(cmdOpts, flags.PrintErrors)
	var err error
	if _, err = parser.Parse(); err != nil {
		if !flags.WroteHelp(err) {
			parser.WriteHelp(os.Stdout)
			return nil, err
		}
	}
	return cmdOpts, nil
}
