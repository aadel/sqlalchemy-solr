class TestSteps:

    settings = None

    def __init__(self, settings):
        self.settings = settings

    def login(self, session) -> dict:
        login_data = {
            "username": self.settings["SUPERSET_USER"],
            "password": self.settings["SUPERSET_PASS"],
            "provider": "db",
            "refresh": True,
        }
        login_response = session.post(
            self.settings["SUPERSET_URI"] + "/api/v1/security/login", json=login_data
        )
        headers = {"Authorization": "Bearer " + login_response.json()["access_token"]}

        return headers

    def create_database(self, session, headers):
        dbs_response = session.get(
            self.settings["SUPERSET_URI"] + "/api/v1/database", headers=headers
        )
        dbs = dbs_response.json()["result"]
        db = list(
            filter(
                lambda db: db["database_name"]
                == self.settings["SUPERSET_DATABASE_NAME"],
                dbs,
            )
        )
        if len(db) == 1:
            self.delete_database(session, headers, db[0]["id"])
        creation_params = {
            "sqlalchemy_uri": "solr://solr:8983/solr/"
            + self.settings["SOLR_WORKER_COLLECTION_NAME"],
            "database_name": "sales_test_",
        }
        db_creation_response = session.post(
            self.settings["SUPERSET_URI"] + "/api/v1/database",
            headers=headers,
            json=creation_params,
        )
        return db_creation_response

    def get_database(self, session, headers, name):
        dbs_response = session.get(
            self.settings["SUPERSET_URI"] + "/api/v1/database", headers=headers
        )
        dbs = dbs_response.json()["result"]
        db = list(
            filter(
                lambda db: db["database_name"]
                == self.settings["SUPERSET_DATABASE_NAME"],
                dbs,
            )
        )
        return db

    def delete_database(self, session, headers, id):
        response = session.delete(
            self.settings["SUPERSET_URI"] + "/api/v1/database/" + str(id),
            headers=headers,
        )
        if response.status_code != 200:
            datasets_response = session.get(
                self.settings["SUPERSET_URI"] + "/api/v1/dataset", headers=headers
            )
            datasets = datasets_response.json()["result"]
            related_datasets = list(
                filter(
                    lambda dataset: dataset["database"]["database_name"]
                    == self.settings["SUPERSET_DATABASE_NAME"],
                    datasets,
                )
            )
            for dataset in related_datasets:
                self.delete_dataset(session, headers, dataset["id"])
            response = session.delete(
                self.settings["SUPERSET_URI"] + "/api/v1/database/" + str(id),
                headers=headers,
            )
            if response.status_code != 200:
                raise Exception("Could not delete database")

    def create_dataset(self, session, headers, database_id):
        datasets_response = session.get(
            self.settings["SUPERSET_URI"] + "/api/v1/dataset", headers=headers
        )
        datasets = datasets_response.json()["result"]
        dataset = list(
            filter(
                lambda dataset: dataset["table_name"]
                == self.settings["SUPERSET_DATABASE_NAME"],
                datasets,
            )
        )
        if len(dataset) == 1:
            self.delete_dataset(session, headers, dataset[0]["id"])
        creation_params = {
            "database": database_id,
            "table_name": self.settings["SUPERSET_DATABASE_NAME"],
        }
        dataset_creation_response = session.post(
            self.settings["SUPERSET_URI"] + "/api/v1/dataset",
            headers=headers,
            json=creation_params,
        )
        return dataset_creation_response

    def delete_dataset(self, session, headers, id):
        response = session.delete(
            self.settings["SUPERSET_URI"] + "/api/v1/dataset/" + str(id),
            headers=headers,
        )
        if response.status_code != 200:
            raise Exception("Could not delete dataset")
