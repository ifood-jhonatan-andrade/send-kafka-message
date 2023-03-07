Install dependencies

```shell
sh install.sh
```


Send message and save alias

```shell
python main.py publish \
 --context merchant-nv \
 --topic catalog.product-datasheet-failover-job-input \
 --key jobId \
 --path-schema schema.avsc \
 --path-message message.json \
 --alias send-message-topic \
 --save

```

Send the same message

````shell
python main.py publish --alias send-message-topic
````

Send the same message by editing only the payload content
````shell
python main.py publish --alias send-message-topic \
 --path-message message.json \
 --save

````
