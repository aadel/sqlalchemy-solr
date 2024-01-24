import json
import os

import pysolr

from requests.auth import HTTPBasicAuth

class SalesFixture:

    TIMEOUT = 10
    COLLECTION_NAME = "sales_test_"
    FILE_NAME = "sales.jsonl"
    solr = None

    def __init__(self, settings):
        self.base_url = settings["SOLR_BASE_URL"]
        if settings["SOLR_USER"]:
            auth = HTTPBasicAuth(settings["SOLR_USER"], settings["SOLR_PASS"])
        else:
            auth = None

        self.solr = pysolr.Solr(
            self.base_url + "/" + self.COLLECTION_NAME,
            always_commit=True,
            timeout=SalesFixture.TIMEOUT,
            auth=auth
        )

    def index_jsonl(self, file_path):

        data = []

        # make sure Solr is up and running
        self.solr.ping()

        with open(file_path, encoding="utf-8") as f:
            jsonl_content = f.readlines()
            data = [json.loads(line) for line in jsonl_content]

        self.truncate_collection()

        # index data
        self.solr.add(data)

    def truncate_collection(self):
        try:
            self.solr.delete(q="*:*")
        except pysolr.SolrError:
            pass

    def index(self):
        path = os.path.join(os.path.dirname(__file__), self.FILE_NAME)
        self.index_jsonl(path)
