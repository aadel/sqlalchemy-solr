import os
import json
from requests import Session


class TestDBCreation:
    SOLR_WORKER_COLLECTION_NAME = "sales_test_"
    SUPERSET_URI = 'http://superset:8088'
    SUPERSET_USER = os.environ['SUPERSET_USER']
    SUPERSET_PASS = os.environ['SUPERSET_PASS']
    SUPERSET_DATABASE_NAME = 'sales_test_'

    session = Session()

    def test_db_creation(self):

        login_data = {
            "username": self.SUPERSET_USER,
            "password": self.SUPERSET_PASS,
            "provider": "db", "refresh": True
        }
        login_response = self.session.post(self.SUPERSET_URI + '/api/v1/security/login', 
            json=login_data)
        headers = {"Authorization": "Bearer " + login_response.json()['access_token']}
        datasets_response = self.session.get(self.SUPERSET_URI + '/api/v1/dataset',
            headers=headers)
        datasets = datasets_response.json()['result']
        for dataset in datasets:
            if dataset['table_name'] == self.SUPERSET_DATABASE_NAME:
                self.delete_dataset(headers, dataset['id'])
        dbs_response = self.session.get(self.SUPERSET_URI + '/api/v1/database',
            headers=headers)
        dbs = dbs_response.json()['result']
        for db in dbs:
            if db['database_name'] == self.SUPERSET_DATABASE_NAME:
                db_id = db['id']
        if db_id == None:
            raise KeyError
        creation_params = {
            "database": db_id,
            "table_name": self.SUPERSET_DATABASE_NAME}
        dataset_creation_response = self.session.post(self.SUPERSET_URI + '/api/v1/dataset', 
            headers=headers, json=creation_params)
        assert dataset_creation_response.status_code == 201

    def delete_dataset(self, headers, id):
        response = self.session.delete(self.SUPERSET_URI + '/api/v1/dataset/' + str(id), 
            headers=headers)
        assert response.status_code == 200
