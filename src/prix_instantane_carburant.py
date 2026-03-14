# Importe requests pour appeler l'API HTTP.
import requests

# Importe json pour écrire les données dans un fichier JSON.
import json

# Importe os pour gérer les dossiers et chemins.
import os

# Importe la fonction stockage_fichier depuis le fichier file_writing.py
from utils.file_writing import stockage_fichier

# Importe la fonction stocker_dans_bdd depuis le fichier db.py
from bdd.db import stocker_dans_bdd

# Import des fonctions de lecture de configuration depuis config_reader.py.
from utils.config_reader import read_configuration

# Nom de la table d'accueil des datas prix carburant dans la bdd
nom_table = "prix_instante_raw"


def telecharger_donnees_economie_gouv(dataset):
    # Affiche un message pour indiquer le début du téléchargement.
    print("Télécharger les données")

    # Liste qui contiendra toutes les lignes récupérées depuis l'API.
    toutes_les_data = []

    # Variable initiale du nombre total d'enregistrements.
    total_count = 1

    # Nombre de lignes récupérées par appel API.
    step = 100

    # Décalage de départ pour la pagination.
    offset = 0

    # URL générique construite à partir du nom du dataset.
    # On garde {limit} et {offset} dans la chaîne pour les remplacer plus tard avec .format(...).
    url = f"https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={{limit}}&offset={{offset}}"

    # Boucle infinie pour paginer tant qu'il reste des données à récupérer.
    while True:
        # Appelle l'API en remplaçant {limit} et {offset} dans l'URL.
        r = requests.get(url.format(limit=step, offset=offset))

        # Lève une erreur si la requête HTTP a échoué.
        r.raise_for_status()

        # Transforme la réponse JSON en dictionnaire Python.
        data = r.json()

        # Ajoute les résultats récupérés à la liste principale.
        toutes_les_data += data["results"]

        # Récupère le nombre total d'enregistrements disponibles.
        total_count = data["total_count"]

        # Met à jour l'offset pour l'appel suivant.
        offset += len(data["results"])

        # Si on a tout récupéré, on arrête la boucle.
        if total_count - len(toutes_les_data) <= 0:
            break

        # Sécurité pour éviter de dépasser 10 000 lignes.
        if offset + step > 10000:
            break

    # Retourne toutes les données récupérées.
    return toutes_les_data
