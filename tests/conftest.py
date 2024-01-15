import os

import pytest


@pytest.fixture(scope="session")
def settings():
    solr_worker_collection_name = "sales_test_"
    if os.environ.get("SOLR_USER"):
        solr_connection_uri = f'solr://{os.environ.get("SOLR_USER")}:{os.environ.get("SOLR_PASS")}@solr:8983/solr/' \
            + solr_worker_collection_name
    else:
        solr_connection_uri = 'solr://solr:8983/solr/' + solr_worker_collection_name

    return {
        "HOST": "solr",
        "PORT": 8983,
        "PROTO": "http://",
        "SOLR_USER": os.environ.get("SOLR_USER"),
        "SOLR_PASS": os.environ.get("SOLR_PASS"),
        "SERVER_PATH": "solr",
        "SOLR_BASE_URL": "http://solr:8983/solr",
        "SOLR_CONNECTION_URI": solr_connection_uri,
        "SOLR_WORKER_COLLECTION_NAME": solr_worker_collection_name,
        "SUPERSET_URI": "http://superset:8088",
        "SUPERSET_USER": os.environ["SUPERSET_USER"],
        "SUPERSET_PASS": os.environ["SUPERSET_PASS"],
        "SUPERSET_DATABASE_NAME": "sales_test_",
    }
