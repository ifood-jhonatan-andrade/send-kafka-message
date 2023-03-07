import json
import yaml

PATH_ALIAS = "configs/alias.yaml"
PATH_SCHEMA = "configs/alias_schema.txt"


def save(alias, path_context, topic, key, schema, message):
    dict_alias = get()

    dict_alias[alias] = {
        "alias": alias,
        "path_context": path_context,
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


def get_schema():
    with open(PATH_SCHEMA, "r") as f:
        schema = f.read()
        data = json.loads(schema)

        return data


def get():
    with open(PATH_ALIAS, "r") as f:
        alias = yaml.safe_load(f)

        if alias is None:
            alias = dict()

        return alias
