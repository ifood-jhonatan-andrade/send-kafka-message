Install dependencies

```shell
sh install.sh
```

## Create Context

```shell
python main.py context --context context01 \
--schema-registry-url https://localhost:9093 \
--schema-registry-key <SCHEMA_REGISTRY_KEY> \
--schema-registry-secret <SCHEMA_REGISTRY_SECRET> \
--kafka-api-key <KAFKA_API_KEY> \
--kafka-api-secret <KAFKA_API_SECRET> \
--kafka-server localhost:9092 \
--kafka-auth PLAIN

```

## Send message and save alias

1. Create a file called `message.json` and add the message payload
2. Create a file called `schema.avsc` and add the schema avro
3. Run this command
```shell
python main.py publish \
 --context context01 \
 --topic <TOPIC> \
 --key <KEY> \
 --path-schema schema.avsc \
 --path-message message.json \
 --alias send-message-topic \
 --save

```

# Send the same message
You can now create alias for your submissions and send them repeatedly

````shell
python main.py publish --alias send-message-topic
````

Send the same message by editing only the payload content
````shell
python main.py publish --alias send-message-topic \
 --path-message message.json \
 --save

````

All submissions will be stored in alias.yaml files. In which you can use them to edit and make new submissions

