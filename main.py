import argparse
import json

from send import send
from alias import save, get, get_schema

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
    args = parser.parse_args()

    action_args = args.action
    alias_args = args.alias
    context_args = args.context
    topic_args = args.topic
    key_args = args.key
    path_schema_args = args.path_schema
    path_message_args = args.path_message
    save_args = args.save

    if action_args == "publish":
        if alias_args is not None and context_args is None:
            print("Loading Alias...")
            alias = get()[alias_args]
            print("Loading Alias Schema...")
            alias_schema = get_schema()[alias_args]
            print("Alias Found with Successfully...")

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

            send(
                alias["topic"],
                alias["path_context"],
                message_json_update,
                alias["key"],
                schema_update
            )
            print("Message Sent Successfully")
            if save_args:
                save(
                    alias_args,
                    alias["path_context"],
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

            path_context = f"configs/contexts/{context_args}.yaml"
            print("Send Message...")
            send(topic_args, path_context, message_json, key_args, schema)
            print("Message Sent Successfully")
            if save_args:
                save(alias_args, path_context, topic_args, key_args, schema, message_json)
                print("Alias Saved Successfully")
