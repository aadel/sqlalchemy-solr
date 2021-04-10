import json
import os

import pysolr


class SalesFixture:

    TIMEOUT = 10
    COLLECTION_NAME = "sales_test_"
    FILE_NAME = "sales.jsonl"
    solr = None

    def __init__(self, base_url):
        self.base_url = base_url
        self.solr = pysolr.Solr(
            self.base_url + "/" + self.COLLECTION_NAME,
            always_commit=True,
            timeout=SalesFixture.TIMEOUT,
        )

    def index_jsonl(self, file_path):

        data = []

        # make sure Solr is up and running
        self.solr.ping()

        with open(file_path) as f:
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
