import json
import yaml

PATH_ALIAS = "configs/alias.yaml"
PATH_SCHEMA = "configs/alias_schema.txt"
PATH_CONTEXTS = "configs/contexts.yaml"


def save(alias, context, topic, key, schema, message):
    dict_alias = get()

    dict_alias[alias] = {
        "alias": alias,
        "context": context,
        "topic": topic,
        "key": key
    }
    data = json.loads(json.dumps(dict_alias))

    dict_schema = get_schema()
    dict_schema[alias] = {
        "schema": schema,
        "message": message
    }

    data_schema = json.dumps(dict_schema)

    with open(PATH_ALIAS, "w") as f:
        yaml.dump(data, f)

    with open(PATH_SCHEMA, "w") as f:
        f.write(data_schema)


def save_context(
        CONTEXT,
        KAFKA_SCHEMA_REGISTRY_URL,
        KAFKA_SCHEMA_REGISTRY_API_KEY,
        KAFKA_SCHEMA_REGISTRY_API_SECRET,
        KAFKA_API_KEY,
        KAFKA_API_SECRET,
        KAFKA_BOOTSTRAP_SERVER,
        KAFKA_AUTH_MODULE,
):
    context = get_contexts()

    context[CONTEXT] = {
        "KAFKA_SCHEMA_REGISTRY_URL": KAFKA_SCHEMA_REGISTRY_URL,
        "KAFKA_SCHEMA_REGISTRY_API_KEY": KAFKA_SCHEMA_REGISTRY_API_KEY,
        "KAFKA_SCHEMA_REGISTRY_API_SECRET": KAFKA_SCHEMA_REGISTRY_API_SECRET,
        "KAFKA_API_KEY": KAFKA_API_KEY,
        "KAFKA_API_SECRET": KAFKA_API_SECRET,
        "KAFKA_BOOTSTRAP_SERVER": KAFKA_BOOTSTRAP_SERVER,
        "KAFKA_AUTH_MODULE": KAFKA_AUTH_MODULE,
    }

    data = json.loads(json.dumps(context))

    with open(PATH_CONTEXTS, "w") as f:
        yaml.dump(data, f)


def get_schema():
    with open(PATH_SCHEMA, "r") as f:
        schema = f.read()
        data = json.loads(schema)

        return data


def get_contexts():
    with open(PATH_CONTEXTS, "r") as f:
        contexts = yaml.safe_load(f)

        if contexts is None:
            contexts = dict()

        return contexts


def get():
    with open(PATH_ALIAS, "r") as f:
        alias = yaml.safe_load(f)

        if alias is None:
            alias = dict()

        return alias
