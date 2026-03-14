# Importe le module json pour lire le fichier de configuration JSON.
import json


def read_configuration():
    # Ouvre le fichier de configuration situé dans src/config.json en mode lecture.
    with open("src/config.json", "r", encoding="utf-8") as f:
        # Charge le JSON en objet Python.
        configuration = json.load(f)

    # Parcourt chaque configuration pour y ajouter le contenu du fichier SQL.
    for config in configuration:
        # Ajoute une clé "sql_creation" contenant le texte SQL lu depuis le fichier.
        config["sql_creation"] = sql_finder(config["fichier_sql"])

    # Retourne la liste des configurations enrichies.
    return configuration


def sql_finder(nom_fichier):
    # Ouvre le fichier SQL situé dans src/sql/.
    with open(f"src/sql/{nom_fichier}", "r", encoding="utf-8") as f:
        # Retourne tout le contenu du fichier SQL sous forme de texte.
        return f.read()
