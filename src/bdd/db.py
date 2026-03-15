# Import de duckdb pour créer et alimenter la base de données.
import duckdb


def stocker_dans_bdd(sql_creation, json_path, db_path, nom_table):
    # Affiche un message pour indiquer le chargement dans la base.
    print("Chargement dans la BDD")

    # Ouvre ou crée la base DuckDB.
    connection = duckdb.connect(db_path)

    # Exécute la création de la table.
    connection.sql(sql_creation)

    # Insère dans la table toutes les lignes lues depuis le fichier JSON.
    connection.sql(
        f"""
        INSERT INTO {nom_table}
        SELECT * FROM read_json_auto('{json_path}')
        """
    )

    # Ferme proprement la connexion à la base.
    connection.close()

    # Message final.
    print("Terminé : JSON dans data/ et base DuckDB dans bdd/")


if __name__ == "__main__":
    print("ceci est un test de stocker_dans_bdd")
