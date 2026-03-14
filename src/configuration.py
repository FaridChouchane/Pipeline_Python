import requests

class Configuration:

    def __init__(self, input_config):
        self.db_path = input_config["db_path"]
        self.fichier_sql = input_config["fichier_sql"]
        self.json_path = input_config["json_path"]
        self.type_api = input_config["type_api"]
        self.dataset = input_config["dataset"]
        self.sql_creation = input_config["sql_creation"]
        self.nom_table = input_config["nom_table"]

    def telecharger(self):
        if self.type_api == "conso_gouv":
            return self.telecharger_data_gouv()
        elif self.type_api == "economie_gouv":
            return self.telecharger_donnees_economie_gouv()
        else:
            raise ValueError(f"La valeur de type_api = {self.type_api} n'est pas connue")

    def telecharger_data_gouv(self):
        print("Télécharger les données")

        toutes_les_data = []
        url = f"https://tabular-api.data.gouv.fr/api/resources/{self.dataset}/data/?Date__exact=2025-12-31"

        while url:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            toutes_les_data += data["data"]
            url = data["links"].get("next")

        return toutes_les_data

    def telecharger_donnees_economie_gouv(self):
        print("Télécharger les données")

        toutes_les_data = []
        total_count = 1
        step = 100
        offset = 0

        url = f"https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/{self.dataset}/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={{limit}}&offset={{offset}}"

        while True:
            r = requests.get(url.format(limit=step, offset=offset))
            r.raise_for_status()
            data = r.json()

            toutes_les_data += data["results"]
            total_count = data["total_count"]
            offset += len(data["results"])

            if total_count - len(toutes_les_data) <= 0:
                break

            if offset + step > 10000:
                break

        return toutes_les_data
