import requests
import json
import abc

# ----------------------------------------------------------------------------------------
class Configuration(abc.ABC):

    def __init__(self, input_config):
        self.input_config = input_config


    def telecharger(self):
        raise NotImplementedError()

# ----------------------------------------------------------------------------------------
class BeautifulPrinter():

    def __init__(self, input_config):
        self.input_config = input_config

    def print(self):
        print(json.dumps(self.input_config, indent = 4))

# ---------------------------------------------------------------------------------------
class EconomieGouvConfiguration(Configuration, BeautifulPrinter):

    def __init__(self, input_config):
        super().__init__(input_config)
        self.db_path = self.input_config["db_path"]
        self.fichier_sql = self.input_config["fichier_sql"]
        self.json_path = self.input_config["json_path"]
        self.type_api = self.input_config["type_api"]
        self.dataset = self.input_config["dataset"]
        self.sql_creation =self.input_config["sql_creation"]
        self.nom_table = self.input_config["nom_table"]
        self.url = f"https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/{self.dataset}/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={{limit}}&offset={{offset}}"


    def telecharger(self):
        print("Télécharger les données")

        toutes_les_data = []
        total_count = 1
        step = 100
        offset = 0

        while True:
            r = requests.get(self.url.format(limit=step, offset=offset))
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

# ----------------------------------------------------------------------------------------
class DataGouvConfiguration(Configuration):

    def __init__(self, input_config):
        super().__init__(input_config)
        self.db_path = self.input_config["db_path"]
        self.fichier_sql = self.input_config["fichier_sql"]
        self.json_path = self.input_config["json_path"]
        self.type_api = self.input_config["type_api"]
        self.dataset = self.input_config["dataset"]
        self.sql_creation =self.input_config["sql_creation"]
        self.nom_table = self.input_config["nom_table"]
        self.url = f"https://tabular-api.data.gouv.fr/api/resources/{self.dataset}/data/?Date__exact=2025-12-31"

    def telecharger(self):
        print("Télécharger les données")
        toutes_les_data = []
        url = self.url
        while url:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            toutes_les_data += data["data"]
            url = data["links"].get("next")

        return toutes_les_data

# ----------------------------------------------------------------------------------------
