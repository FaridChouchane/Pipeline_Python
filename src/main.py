# Importe requests pour appeler l'API HTTP.
# Ici, requests n'est pas utilisé directement dans main.py,
# mais je le laisse pour modifier le moins possible ton script.
import requests

# Importe json pour écrire les données dans un fichier JSON.
# Ici aussi, json n'est pas utilisé directement dans main.py,
# mais je le laisse pour rester proche de ton code.
import json

# Importe os pour gérer les dossiers et chemins.
import os

# Importe la fonction stockage_fichier depuis le fichier file_writing.py.
from utils.file_writing import stockage_fichier

# Importe la fonction stocker_dans_bdd depuis le fichier db.py.
from bdd.db import stocker_dans_bdd

# Importe les fonctions de lecture de configuration depuis config_reader.py.
from utils.config_reader import read_configuration

# Importe la fonction de téléchargement des prix carburants.
from prix_instantane_carburant import telecharger_donnees_economie_gouv

# Importe la fonction de téléchargement des données de consommation gaz/élec.
from consommation_brute_quotidienne_gaz_elec import telecharger_data_gouv


# Crée le dossier "data" s'il n'existe pas déjà.
os.makedirs("data", exist_ok=True)

# Crée le dossier "bdd" s'il n'existe pas déjà.
os.makedirs("bdd", exist_ok=True)

# Lit le fichier de configuration.
configuration = read_configuration()

# Parcourt chaque bloc de configuration.
for config in configuration:
    # Vérifie le type d'API du bloc courant.
    if config["type_api"] == "economie_gouv":
        # Télécharge les données depuis l'API prix carburants.
        resultat = telecharger_donnees_economie_gouv(config["dataset"])

        # Définit le nom de la table cible pour les carburants.
        nom_table = "prix_instante_raw"

    elif config["type_api"] == "conso_gouv":
        # Télécharge les données depuis l'API consommation gaz/élec.
        resultat = telecharger_data_gouv(config["dataset"])

        # Définit le nom de la table cible pour la conso gaz/élec.
        nom_table = "consommation_brute_quotidienne_gaz_elec_raw"

    else:
        # Lève une erreur si le type d'API n'est pas reconnu.
        raise ValueError(f"Le type d'API {config['type_api']} est inconnu")

    # Stocke les données téléchargées dans le fichier JSON défini dans la configuration.
    stockage_fichier(resultat, config["json_path"])

    # Charge les données dans DuckDB en respectant l'ordre des paramètres attendu par db.py :
    # 1. le SQL de création de table
    # 2. le chemin du fichier JSON
    # 3. le chemin de la base DuckDB
    # 4. le nom de la table
    stocker_dans_bdd(
        config["sql_creation"],
        config["json_path"],
        config["db_path"],
        nom_table
    )
