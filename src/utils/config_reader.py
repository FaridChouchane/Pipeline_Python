import json
from configuration import EconomieGouvConfiguration, DataGouvConfiguration

def read_configuration():
    out = []

    with open("src/config.json", "r", encoding="utf-8") as f:
        configuration = json.load(f)

    for config in configuration:
        config["sql_creation"] = sql_finder(config["fichier_sql"])
        if config["type_api"] == "economie_gouv":
            out.append(EconomieGouvConfiguration(config))
        elif config["type_api"] == "conso_gouv":
            out.append(DataGouvConfiguration(config))
        else:
            raise ValueError(f"La clé type_api = {config['type_api']} n 'est pas connue")
    return out


def sql_finder(nom_fichier):
    with open(f"src/sql/{nom_fichier}", "r", encoding="utf-8") as f:
        return f.read()
