from .settings import (SUPERSET_USER, SUPERSET_PASS, 
    SUPERSET_URI, SUPERSET_DATABASE_NAME, SOLR_WORKER_COLLECTION_NAME)

def login(session) -> dict:
    login_data = {
        "username": SUPERSET_USER,
        "password": SUPERSET_PASS,
        "provider": "db", "refresh": True
    }
    login_response = session.post(SUPERSET_URI + '/api/v1/security/login', 
        json=login_data)
    headers = {"Authorization": "Bearer " + login_response.json()['access_token']}

    return headers

def create_database(session, headers):
    dbs_response = session.get(SUPERSET_URI + '/api/v1/database',
        headers=headers)
    dbs = dbs_response.json()['result']
    db = list(filter(lambda db: db['database_name'] == SUPERSET_DATABASE_NAME, dbs))
    if len(db) == 1:
        delete_database(session, headers, db[0]['id'])
    creation_params = {
        "sqlalchemy_uri": "solr://solr:8983/solr/" + SOLR_WORKER_COLLECTION_NAME,
        "database_name": "sales_test_"}
    db_creation_response = session.post(SUPERSET_URI + '/api/v1/database', 
        headers=headers, json=creation_params)
    return db_creation_response

def get_database(session, headers, name):
    dbs_response = session.get(SUPERSET_URI + '/api/v1/database',
        headers=headers)
    dbs = dbs_response.json()['result']
    db = list(filter(lambda db: db['database_name'] == SUPERSET_DATABASE_NAME, dbs))
    return db

def delete_database(session, headers, id):
    response = session.delete(SUPERSET_URI + '/api/v1/database/' + str(id), 
        headers=headers)
    if response.status_code != 200:
        datasets_response = session.get(SUPERSET_URI + '/api/v1/dataset',
            headers=headers)
        datasets = datasets_response.json()['result']
        related_datasets = list(filter(lambda dataset: dataset['database']['database_name'] == SUPERSET_DATABASE_NAME, datasets))
        for dataset in related_datasets:
            delete_dataset(session, headers, dataset['id'])
        response = session.delete(SUPERSET_URI + '/api/v1/database/' + str(id), 
            headers=headers)
        if response.status_code != 200:
            raise Exception("Could not delete database")

def create_dataset(session, headers, database_id, recreate=False):
    datasets_response = session.get(SUPERSET_URI + '/api/v1/dataset',
        headers=headers)
    datasets = datasets_response.json()['result']
    dataset = list(filter(lambda dataset: dataset['table_name'] == SUPERSET_DATABASE_NAME, datasets))
    if len(dataset) == 1 and recreate:
        delete_dataset(session, headers, dataset[0]['id'])
    creation_params = {
        "database": database_id,
        "table_name": SUPERSET_DATABASE_NAME}
    dataset_creation_response = session.post(SUPERSET_URI + '/api/v1/dataset', 
        headers=headers, json=creation_params)
    return dataset_creation_response

def delete_dataset(session, headers, id):
    response = session.delete(SUPERSET_URI + '/api/v1/dataset/' + str(id), 
        headers=headers)
    if response.status_code != 200:
        raise Exception("Could not delete dataset")
