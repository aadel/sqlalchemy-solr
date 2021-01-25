import os

SOLR_BASE_URL = 'http://solr:8983/solr'
SOLR_CONNECTION_URI = 'solr://solr:8983/solr'
SOLR_WORKER_COLLECTION_NAME = "sales_test_"
SUPERSET_URI = 'http://superset:8088'
SUPERSET_USER = os.environ['SUPERSET_USER']
SUPERSET_PASS = os.environ['SUPERSET_PASS']
SUPERSET_DATABASE_NAME = 'sales_test_'