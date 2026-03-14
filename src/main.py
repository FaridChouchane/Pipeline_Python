import requests
import json
import os

from utils.file_writing import stockage_fichier
from bdd.db import stocker_dans_bdd
from utils.config_reader import read_configuration

os.makedirs("data", exist_ok=True)
os.makedirs("bdd", exist_ok=True)

configuration = read_configuration()

for config in configuration:
    resultat = config.telecharger()

    stockage_fichier(resultat, config.json_path)

    stocker_dans_bdd(
        config.sql_creation,
        config.json_path,
        config.db_path,
        config.nom_table
    )
