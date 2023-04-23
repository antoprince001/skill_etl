import json


def get_config_json(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as config:
        return json.load(config)
