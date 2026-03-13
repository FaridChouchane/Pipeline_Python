# Importe la librairie requests pour appeler l'API HTTP.
import requests
# Importe duckdb pour créer et alimenter une base de données DuckDB.
import duckdb
# Importe json pour écrire les données récupérées dans un fichier JSON.
import json
# Importe os pour gérer les chemins et créer les dossiers si besoin.
import os

# URL de l'API publique data.gouv.fr.
# Elle récupère les données de consommation brute quotidienne gaz/électricité
# pour la date exacte 2024-10-31.
url = 'https://tabular-api.data.gouv.fr/api/resources/cfc27ff9-1871-4ee8-be64-b9a290c06935/data/?Date__exact=2025-12-31'

# Liste Python qui stockera toutes les lignes récupérées depuis l'API.
toutes_les_data = []


# Crée le dossier "data" s'il n'existe pas encore.
os.makedirs("data", exist_ok=True)

# Crée le dossier "bdd" s'il n'existe pas encore.
os.makedirs("bdd", exist_ok=True)

# Chemin du fichier JSON généré dans le dossier data.
json_path = "data/consommation_brute_quotidienne_gaz_elec.json"

# Chemin du fichier de base DuckDB dans le dossier bdd.
db_path = "bdd/bdd_cours_python_avance.duckdb"


# Affiche un message pour indiquer le chargement dans la base.
print("#" * 25)
print("Chargement dans la BDD")
print("#" * 25)

# Ouvre ou crée la base DuckDB dans le dossier bdd.
connection = duckdb.connect(db_path)

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

# Exécute la création de la table dans DuckDB.
connection.sql(sql_creation)

# Insère dans la table toutes les lignes lues depuis le fichier JSON.
# read_json_auto lit automatiquement la structure du fichier.
connection.sql(
    f"""
    INSERT INTO consommation_brute_quotidienne_gaz_elec_raw
    SELECT * FROM read_json_auto('{json_path}')
    """
)

# Ferme proprement la connexion à la base.
connection.close()

# Message final pour confirmer que tout est terminé.
print("Terminé : JSON dans data/ et base DuckDB dans bdd/")
