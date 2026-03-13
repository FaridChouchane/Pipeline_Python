# Importe json pour écrire les données dans un fichier JSON.
import json

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
