Instalar dependencias

```shell
sh install.sh
```


Enviar Mensagem e Salva Alias

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

Enviar a mesma mensagem

````shell
python main.py publish --alias send-message-topic
````

Enviar a mesma mensagem editando apenas o conteúdo do payload
````shell
python main.py publish --alias send-message-topic \
 --path-message message.json \
 --save

````
