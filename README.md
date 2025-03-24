Install dependencies

```shell
sh install.sh
```

# Run Local

### Docker

```shell
docker-compose up -d [--build]
```

### Create Topic

```shell
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic catalog.catalog-products-ingestor-average
```

### Create Context

```shell
docker-compose exec kafka-producer python3 main.py context --context context-local --schema-registry-url http://schema-registry:8081 --kafka-server kafka:9092 
```

### Send message and save alias

```shell
docker-compose exec kafka-producer python3 main.py publish --context context-local --topic catalog.catalog-products-ingestor-average --key itemId
 --path-schema collection/schema.avsc --path-message collection/message.json --alias send-message-topic --save
```

### Send the same message

```shell
docker-compose exec kafka-producer python3 main.py publish --alias send-message-topic
```

Send the same message by editing only the payload content

```shell
docker-compose exec kafka-producer python3 main.py publish --alias send-message-topic --path-message collection/message.json --save
```

# Run Production

## Create Context

```shell
python3 main.py context --context context01 \
--schema-registry-url https://localhost:9093 \
--schema-registry-key <SCHEMA_REGISTRY_KEY> \
--schema-registry-secret <SCHEMA_REGISTRY_SECRET> \
--kafka-api-key <KAFKA_API_KEY> \
--kafka-api-secret <KAFKA_API_SECRET> \
--kafka-server localhost:9092 \
--kafka-auth PLAIN

```

## Send message and save alias

1. Create a file called `collection/message.json` and add the message payload
2. Create a file called `collection/schema.avsc` and add the schema avro
3. Run this command
```shell
python3 main.py publish \
 --context context01 \
 --topic <TOPIC> \
 --key <PAYLOAD_PROPERTY> \
 --path-schema collection/schema.avsc \
 --path-message collection/message.json \
 --alias send-message-topic \
 --save

```

# Send the same message
You can now create alias for your submissions and send them repeatedly

````shell
python3 main.py publish --alias send-message-topic
````

Send the same message by editing only the payload content
````shell
python3 main.py publish --alias send-message-topic --path-message collection/message.json --save

````

All submissions will be stored in alias.yaml files. In which you can use them to edit and make new submissions

