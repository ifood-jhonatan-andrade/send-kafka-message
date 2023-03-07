from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
import yaml
import argparse


class KafkaMessage:
    def fromDict(self, dictionary):
        for key in dictionary:
            if type(dictionary[key]) is dict:
                values = list(dictionary[key].values())
                value = values[0]
                setattr(self, key, value)
            elif type(dictionary[key]) is list:
                result = []
                for k in dictionary[key]:
                    result.append(KafkaMessage.fromDict(KafkaMessage(), dictionary=k).toDict(None))
                setattr(self, key, result)
            else:
                setattr(self, key, dictionary[key])
        return self

    def toDict(self, ctx):
        return self.__dict__


class KafkaClient:
    def __init__(self,
                 KAFKA_SCHEMA_REGISTRY_URL,
                 KAFKA_SCHEMA_REGISTRY_API_KEY,
                 KAFKA_SCHEMA_REGISTRY_API_SECRET,
                 KAFKA_API_KEY,
                 KAFKA_API_SECRET,
                 KAFKA_BOOTSTRAP_SERVER,
                 KAFKA_AUTH_MODULE,
                 KAFKA_TOPIC,
                 KEY_MESSAGE):
        self.KAFKA_SCHEMA_REGISTRY_URL = KAFKA_SCHEMA_REGISTRY_URL
        self.KAFKA_SCHEMA_REGISTRY_API_KEY = KAFKA_SCHEMA_REGISTRY_API_KEY
        self.KAFKA_SCHEMA_REGISTRY_API_SECRET = KAFKA_SCHEMA_REGISTRY_API_SECRET
        self.KAFKA_API_KEY = KAFKA_API_KEY
        self.KAFKA_API_SECRET = KAFKA_API_SECRET
        self.KAFKA_BOOTSTRAP_SERVER = KAFKA_BOOTSTRAP_SERVER
        self.KAFKA_AUTH_MODULE = KAFKA_AUTH_MODULE
        self.KAFKA_TOPIC = KAFKA_TOPIC
        self.KEY_MESSAGE = KEY_MESSAGE

    def execute(self, msg, avro):
        schema_registry_conf = {
            'url': self.KAFKA_SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': self.KAFKA_SCHEMA_REGISTRY_API_KEY + ':' + self.KAFKA_SCHEMA_REGISTRY_API_SECRET,
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        avro_serializer = AvroSerializer(schema_registry_client, avro, KafkaMessage.toDict)
        producer_config = {
            'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVER,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer,
            'client.id': 'datasheet-script',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': self.KAFKA_AUTH_MODULE,
            'sasl.username': self.KAFKA_API_KEY,
            'sasl.password': self.KAFKA_API_SECRET,
        }
        producer = SerializingProducer(producer_config)

        producer.produce(topic=self.KAFKA_TOPIC, key=msg.__getattribute__(self.KEY_MESSAGE), value=msg)
        producer.flush()


def send(topic, context, message, key, schema):

    client = KafkaClient(
        KAFKA_SCHEMA_REGISTRY_URL=context["KAFKA_SCHEMA_REGISTRY_URL"],
        KAFKA_SCHEMA_REGISTRY_API_KEY=context["KAFKA_SCHEMA_REGISTRY_API_KEY"],
        KAFKA_SCHEMA_REGISTRY_API_SECRET=context["KAFKA_SCHEMA_REGISTRY_API_SECRET"],
        KAFKA_API_KEY=context["KAFKA_API_KEY"],
        KAFKA_API_SECRET=context["KAFKA_API_SECRET"],
        KAFKA_BOOTSTRAP_SERVER=context["KAFKA_BOOTSTRAP_SERVER"],
        KAFKA_AUTH_MODULE=context["KAFKA_AUTH_MODULE"],
        KAFKA_TOPIC=topic,
        KEY_MESSAGE=key
    )

    message_data = KafkaMessage().fromDict(dictionary=message)

    client.execute(message_data, schema)
