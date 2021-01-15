import csv
import os
import pysolr

class SalesFixture:

    TIMEOUT = 10
    COLLECTION_NAME = 'sales_test_'
    FILE_NAME = 'sales.csv'
    solr = None
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.solr = pysolr.Solr(self.base_url + '/' + self.COLLECTION_NAME,
            always_commit=True, timeout=SalesFixture.TIMEOUT)
        
    def index_csv(self, file_path):

        data = []

        # make sure Solr is up and running
        self.solr.ping()

        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)

        self.truncate_collection()

        # index data
        self.solr.add(data)

    def truncate_collection(self):
        self.solr.delete(q='*:*')

    def index(self):
        path = os.path.join(os.path.dirname(__file__), self.FILE_NAME)
        self.index_csv(path)
