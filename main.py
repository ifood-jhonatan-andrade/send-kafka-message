import argparse
import json

from send import send
from alias import save, get, get_schema, get_contexts, save_context


def publish():
    if alias_args is not None and context_args is None:
        print("Loading Alias...")
        alias = get()[alias_args]
        print("Loading Alias Schema...")
        alias_schema = get_schema()[alias_args]
        print("Alias Found with Successfully...")
        print("Loading Context...")
        alias_context = get_contexts()[alias["context"]]

        if path_schema_args is not None:
            with open(path_schema_args) as f:
                schema_update = f.read()
        else:
            schema_update = alias_schema["schema"]

        if path_message_args is not None:
            with open(path_message_args) as f:
                message_json_update = json.load(f)
        else:
            message_json_update = alias_schema["message"]

        print(f"Send Message to topic {alias['topic']}...")
        send(
            alias["topic"],
            alias_context,
            message_json_update,
            alias["key"],
            schema_update
        )
        print("Message Sent Successfully")
        if save_args:
            save(
                alias_args,
                alias["context"],
                alias["topic"],
                alias["key"],
                schema_update,
                message_json_update
            )
            print("Alias Saved Successfully")
    else:
        with open(path_schema_args) as f:
            schema = f.read()

        with open(path_message_args) as f:
            message_json = json.load(f)

        alias_context = get_contexts()[context_args]

        print(f"Send Message to topic {topic_args}...")
        send(topic_args, alias_context, message_json, key_args, schema)
        print("Message Sent Successfully")
        if save_args:
            save(alias_args, context_args, topic_args, key_args, schema, message_json)
            print("Alias Saved Successfully")


def create_context():
    save_context(
        context_args,
        KAFKA_SCHEMA_REGISTRY_URL,
        KAFKA_SCHEMA_REGISTRY_API_KEY,
        KAFKA_SCHEMA_REGISTRY_API_SECRET,
        KAFKA_API_KEY,
        KAFKA_API_SECRET,
        KAFKA_BOOTSTRAP_SERVER,
        KAFKA_AUTH_MODULE,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("action", help="context variable")

    parser.add_argument("--save", action='store_true', help="context variable")
    parser.add_argument("--alias", help="context variable")
    parser.add_argument("--context", help="context variable")
    parser.add_argument("--topic", help="topic variable")
    parser.add_argument("--key", help="key variable")
    parser.add_argument("--path-schema", help="key variable")
    parser.add_argument("--path-message", help="key variable")

    parser.add_argument("--schema-registry-url", help="context variable")
    parser.add_argument("--schema-registry-key", help="context variable")
    parser.add_argument("--schema-registry-secret", help="context variable")
    parser.add_argument("--kafka-api-key", help="key variable")
    parser.add_argument("--kafka-api-secret", help="key variable")
    parser.add_argument("--kafka-server", help="key variable")
    parser.add_argument("--kafka-auth", help="key variable")
    args = parser.parse_args()

    action_args = args.action
    alias_args = args.alias
    context_args = args.context
    topic_args = args.topic
    key_args = args.key
    path_schema_args = args.path_schema
    path_message_args = args.path_message
    save_args = args.save

    KAFKA_SCHEMA_REGISTRY_URL = args.schema_registry_url
    KAFKA_SCHEMA_REGISTRY_API_KEY = args.schema_registry_key
    KAFKA_SCHEMA_REGISTRY_API_SECRET = args.schema_registry_secret
    KAFKA_API_KEY = args.kafka_api_key
    KAFKA_API_SECRET = args.kafka_api_secret
    KAFKA_BOOTSTRAP_SERVER = args.kafka_server
    KAFKA_AUTH_MODULE = args.kafka_auth

    if action_args == "publish":
        publish()
    elif action_args == "context":
        create_context()
