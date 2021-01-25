import os
import json
from requests import Session
from .settings import (SUPERSET_USER, SUPERSET_PASS, 
    SUPERSET_URI, SUPERSET_DATABASE_NAME, SOLR_WORKER_COLLECTION_NAME)
from .steps import login, create_database

class TestDBCreation:

    def test_db_creation(self):
        session = Session()
        headers = login(session)
        db_creation_response = create_database(session, headers)
        assert db_creation_response.status_code == 201
