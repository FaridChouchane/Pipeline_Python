# Importe la librairie requests pour appeler l'API HTTP.
import requests
# Importe json pour écrire les données récupérées dans un fichier JSON.
import json
# Importe os pour gérer les chemins et créer les dossiers si besoin.
import os
#Importe la fonction stocker_dans_bdd depuis le fichier db.py
from bdd.db import stocker_dans_bdd


# URL de l'API publique data.gouv.fr.
# Elle récupère les données de consommation brute quotidienne gaz/électricité
# pour la date exacte YYYY-MM-DD.
url = 'https://tabular-api.data.gouv.fr/api/resources/cfc27ff9-1871-4ee8-be64-b9a290c06935/data/?Date__exact=2025-12-31'
# Crée le dossier "data" s'il n'existe pas encore.
os.makedirs("data", exist_ok=True)
# Crée le dossier "bdd" s'il n'existe pas encore.
os.makedirs("bdd", exist_ok=True)
# Chemin du fichier JSON généré dans le dossier data.
json_path = "data/consommation_brute_quotidienne_gaz_elec.json"
# nom de la table d'acceuil des datas conso brute quotidienne gaz & elec dans la bdd
nom_table = "consommation_brute_quotidienne_gaz_elec_raw"


# Requête SQL de création de table.
# CREATE OR REPLACE remplace la table existante si elle a été créée avec un mauvais schéma.
sql_creation = """
CREATE OR REPLACE TABLE consommation_brute_quotidienne_gaz_elec_raw (
    "id" INT,
    "Date - Heure" TIMESTAMP,
    "Date" DATE,
    "Heure" STRING,
    "Consommation brute gaz (MW PCS 0°C) - GRTgaz" INT,
    "Statut - GRTgaz" STRING,
    "Consommation brute gaz (MW PCS 0°C) - Teréga" INT,
    "Statut - Teréga" STRING,
    "Consommation brute gaz totale (MW PCS 0°C)" INT,
    "Consommation brute électricité (MW)- RTE" INT,
    "Statut - RTE" STRING,
    "Consommation brute totale (MW)" INT,
    "flag_ignore" BOOLEAN
)
"""
# Chemin du fichier de base DuckDB dans le dossier bdd.
db_path = "bdd/conso_et_prix_energies_Fr.duckdb"

def telecharger_donnees_conso_gaz_elec(url):
     # Affiche un message dans le terminal pour indiquer le début du téléchargement.
    print("Télécharger les données")
    # Liste Python qui stockera toutes les lignes récupérées depuis l'API.
    toutes_les_data = []
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
    return toutes_les_data

def stockage_fichier(toutes_les_data, json_path):
    # Affiche un message pour indiquer qu'on va écrire le fichier JSON.
    print("Stockage dans le fichier")
    # Ouvre le fichier JSON en écriture dans le dossier data.
    # encoding="utf-8" force l'encodage UTF-8.
    with open(json_path, "w", encoding="utf-8") as f:
        # Parcourt chaque ligne récupérée depuis l'API.
        for line in toutes_les_data:
            # Écrit chaque objet JSON sur une ligne du fichier.
            json.dump(line, f, ensure_ascii=False)
            # Ajoute un retour à la ligne pour avoir un fichier JSON ligne par ligne.
            f.write("\n")


resultat = telecharger_donnees_conso_gaz_elec(url)
stockage_fichier(resultat, json_path)
stocker_dans_bdd(sql_creation, json_path, db_path, nom_table)
