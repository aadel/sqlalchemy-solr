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
        dbs_response = self.session.get(self.SUPERSET_URI + '/api/v1/database',
            headers=headers)
        dbs = dbs_response.json()['result']
        for db in dbs:
            if db['database_name'] == self.SUPERSET_DATABASE_NAME:
                self.delete_database(headers, db['id'])
        creation_params = {
            "sqlalchemy_uri": "solr://solr:8983/solr/" + self.SOLR_WORKER_COLLECTION_NAME,
            "database_name": "sales_test_"}
        db_creation_response = self.session.post(self.SUPERSET_URI + '/api/v1/database', 
            headers=headers, json=creation_params)
        assert db_creation_response.status_code == 201

    def delete_database(self, headers, id):
        response = self.session.delete(self.SUPERSET_URI + '/api/v1/database/' + str(id), 
            headers=headers)
        assert response.status_code == 200