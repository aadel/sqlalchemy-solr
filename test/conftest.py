import os

import pytest


@pytest.fixture(scope="session")
def settings():
    return {
        "HOST": "solr",
        "PORT": 8983,
        "PROTO": "http://",
        "SOLR_USER": os.environ["SOLR_USER"],
        "SOLR_PASS": os.environ["SOLR_PASS"],
        "SERVER_PATH": "solr",
        "SOLR_BASE_URL": "http://solr:8983/solr",
        "SOLR_CONNECTION_URI": "solr://solr:8983/solr",
        "SOLR_WORKER_COLLECTION_NAME": "sales_test_",
        "SUPERSET_URI": "http://superset:8088",
        "SUPERSET_USER": os.environ["SUPERSET_USER"],
        "SUPERSET_PASS": os.environ["SUPERSET_PASS"],
        "SUPERSET_DATABASE_NAME": "sales_test_",
    }
