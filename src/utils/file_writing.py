# Importe json pour écrire les données dans un fichier JSON.
import json


def stockage_fichier(toutes_les_data, json_path):
    # Affiche un message pour indiquer qu'on va écrire les données dans le fichier JSON.
    print("Stockage dans le fichier")

    # Ouvre le fichier JSON en écriture.
    with open(json_path, "w", encoding="utf-8") as f:
        # Parcourt chaque ligne récupérée depuis l'API.
        for line in toutes_les_data:
            # Écrit chaque objet JSON dans le fichier.
            json.dump(line, f, ensure_ascii=False)

            # Ajoute un saut de ligne après chaque objet.
            f.write("\n")
