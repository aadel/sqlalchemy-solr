import os
import json
from requests import Session

SOLR_WORKER_COLLECTION_NAME = "sales_test_"
SUPERSET_URI = 'http://superset:8088'
SUPERSET_USER = 'admin'
SUPERSET_PASS = os.environ['SUPERSET_PASS']
SUPERSET_DATABASE_NAME = 'sales_test_'

session = Session()

def test_db_creation():

    login_data = {
        "password": SUPERSET_PASS,
        "provider": "db",
        "refresh": True,
        "username": SUPERSET_USER
        }
    login_response = session.post(SUPERSET_URI + '/api/v1/security/login', 
        json=login_data)
    headers = {"Authorization": "Bearer " + login_response.json()['access_token']}
    dbs_response = session.get(SUPERSET_URI + '/api/v1/database', headers=headers)
    dbs = dbs_response.json()['result']
    for db in dbs:
        if db['database_name'] == SUPERSET_DATABASE_NAME:
            delete_database(headers, db['id'])
    creation_params = {
        "sqlalchemy_uri": "solr://solr:8983/solr/" + SOLR_WORKER_COLLECTION_NAME,
        "database_name": "sales_test_"}
    db_creation_response = session.post(SUPERSET_URI + '/api/v1/database', 
        headers=headers, json=creation_params)
    assert db_creation_response.status_code == 201

def delete_database(headers, id):
    session.delete(SUPERSET_URI + '/api/v1/database/' + str(id), headers=headers)