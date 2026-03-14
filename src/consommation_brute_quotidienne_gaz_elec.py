# Import librairie requests pour appeler l'API HTTP.
import requests

# Import json pour écrire les données récupérées dans un fichier JSON.
import json

# Importe os pour gérer les chemins et créer les dossiers si besoin.
import os

# Import fonction stocker_dans_bdd depuis le fichier db.py.
from bdd.db import stocker_dans_bdd

# Import fonction stockage_fichier depuis le fichier file_writing.py.
from utils.file_writing import stockage_fichier

# Import des fonctions de lecture de configuration depuis config_reader.py.
from utils.config_reader import read_configuration


# Nom de la table d'accueil des données conso brute quotidienne gaz & elec dans la BDD.
nom_table = "consommation_brute_quotidienne_gaz_elec_raw"


def telecharger_data_gouv(dataset):
    # Affiche un message dans le terminal pour indiquer le début du téléchargement.
    print("Télécharger les données")

    # Liste Python qui stockera toutes les lignes récupérées depuis l'API.
    toutes_les_data = []
    url = f"https://tabular-api.data.gouv.fr/api/resources/{dataset}/data/?Date__exact=2025-12-31"


    # Boucle tant qu'il existe une URL à appeler.
    while url:
        # Envoie une requête HTTP GET sur l'URL courante.
        r = requests.get(url)

        # Stoppe le script avec une erreur si la requête HTTP a échoué.
        r.raise_for_status()

        # Convertit la réponse JSON de l'API en dictionnaire Python.
        data = r.json()

        # Ajoute à la liste principale toutes les lignes contenues dans la clé "data".
        toutes_les_data += data["data"]

        # Récupère le lien de la page suivante s'il existe, sinon None.
        url = data["links"].get("next")

    # Retourne la liste complète des données récupérées.
    return toutes_les_data
