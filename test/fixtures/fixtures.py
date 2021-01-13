import csv
import os

import pysolr

class Fixture:

    TIMEOUT = 10
    BASE_URL = 'http://solr:8983/solr'
    
    solr = None
    
    def index_csv(self, base_url, collection_name, file_path):

        data = []

        self.solr = pysolr.Solr(base_url + '/' + collection_name,
            always_commit=True, timeout=Fixture.TIMEOUT)
        
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

    def index_sales(self, base_url):
        path = os.path.join(os.path.dirname(__file__), "sales.csv")
        self.index_csv(base_url, "sales", path)

fixture = Fixture()
fixture.index_sales(Fixture.BASE_URL)