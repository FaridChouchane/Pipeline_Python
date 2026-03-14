import json
from configuration import Configuration

def read_configuration():
    out = []

    with open("src/config.json", "r", encoding="utf-8") as f:
        configuration = json.load(f)

    for config in configuration:
        config["sql_creation"] = sql_finder(config["fichier_sql"])
        out.append(Configuration(config))

    return out


def sql_finder(nom_fichier):
    with open(f"src/sql/{nom_fichier}", "r", encoding="utf-8") as f:
        return f.read()
