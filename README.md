
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

```bash
# We will the topology as defined in https://debezium.io/docs/tutorial/
export DEBEZIUM_VERSION=1.3
export DEBEZIUM_EXTERNAL_IP=10.0.0.105
docker-compose -f docker-compose.yml up

# Start MySQL connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @inventory-connector.json

# Check messages from a Debezium topic for table `inventory.customers`
docker-compose -f docker-compose-mysql.yaml exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic dbserver1.inventory.customers

# Modify records in the database via MySQL client and check the output in Debezium
docker-compose -f docker-compose-mysql.yaml exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory'

# Run debezium2postgres transformation
debezium2postgres --kafka=$DEBEZIUM_EXTERNAL_IP:9092 --topic=dbserver1.inventory --loglevel=debug --postgres=postgres://user:pwd@10.0.0.105/inventory

# Shut down the cluster
docker-compose -f docker-compose-mysql.yaml down
