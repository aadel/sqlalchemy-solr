import json
from requests import Session
from .settings import (SUPERSET_USER, SUPERSET_PASS, 
    SUPERSET_URI, SUPERSET_DATABASE_NAME, SOLR_WORKER_COLLECTION_NAME)
from .steps import login, create_database, get_database, create_dataset

class TestDBCreation:

    def test_db_creation(self):
        session = Session()
        headers = login(session)
        create_database(session, headers)
        db_response = get_database(session, headers, SUPERSET_DATABASE_NAME)
        database_id = db_response[0]['id']
        dataset_creation_response = create_dataset(session, headers, database_id)
        assert dataset_creation_response.status_code == 201
