
[![Build Status](https://github.com/cybertec-postgresql/debezium2postgres/workflows/Go%20Build%20&%20Test/badge.svg)](https://github.com/cybertec-postgresql/debezium2postgres/actions)
[![Coverage Status](https://coveralls.io/repos/github/cybertec-postgresql/debezium2postgres/badge.svg?branch=main)](https://coveralls.io/github/cybertec-postgresql/debezium2postgres?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/cybertec-postgresql/debezium2postgres)](https://goreportcard.com/report/github.com/cybertec-postgresql/debezium2postgres)

# debezium2postgres
Application to apply CDC log from [Debezium](https://debezium.io/) to the target [PostgreSQL](http://www.postgresql.org/).

# use
`$ ./debezium2postgres --kafka=10.0.0.105:9092 --topic=dbserver1.inventory --loglevel=debug --postgres=postgres://user:pwd@10.0.0.105/inventory`
- `kafka` - URL to the debezium kafka
- `topic` - name of the topic with CDC data or the prefix for such topic names, e.g. `dbserver1.inventory` will consume all topics from server `dbserver1` and database `inventory`
- `loglevel` - output message level, e.g. `trace, debug, info, warn, error, panic`
- `postgres` - PostgreSQL connection URL

:warning: To connect to `kafka` cluster the `advertised.listeners` option should be configured properly. See more https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/

# tutorial

* [mysql](tutorial/mysql/)
* [postgresql](tutorial/postgresql/)
