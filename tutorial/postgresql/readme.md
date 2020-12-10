
# from terminal 1:

  
```console
pavlo@lixdebezium-00:~/debezium2postgres/tutorial/postgresql$ docker-compose up
```
  
  
  

# from terminal 2:

  

1. Create destination database

  

```console

pavlo@lixdebezium-00:~/debezium2postgres/tutorial/postgresql$ docker-compose exec postgres bash -c 'psql -U $POSTGRES_USER postgres -c "create database test template postgres"'

```

  

2. Create schemas

  

```console

pavlo@lixdebezium-00:~/debezium2postgres/tutorial/postgresql$ docker-compose exec postgres bash -c 'pgbench -i -It -U $POSTGRES_USER postgres'

  

pavlo@lixdebezium-00:~/debezium2postgres/tutorial/postgresql$ docker-compose exec postgres bash -c 'pgbench -i -It -U $POSTGRES_USER test'

```

  

3. Activate connector

  

```console

pavlo@lixdebezium-00:~/debezium2postgres/tutorial/postgresql$ curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @inventory-connector.json

```

  

4. Start aggressively changing data in source database and measure time

  

```console

pavlo@lixdebezium-00:~/debezium2postgres/tutorial/postgresql$ time docker-compose exec postgres bash -c 'pgbench -i -Ig -s 10 -U $POSTGRES_USER postgres'

```

  

5. Run CDC consuming and transferring

  

```console

pavlo@pg480:debezium2postgres$ time go run main.go --kafka=10.0.0.105:9092 --topic=dbserver1.public --loglevel=trace --postgres=postgres://postgres:postgres@10.0.0.105/test

```


