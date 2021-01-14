from .fixtures.fixtures import SalesFixture

BASE_URL = 'http://solr:8983/solr'

f = SalesFixture(BASE_URL)

class TestData():
    def test_index_sales_data(self):
        f.truncate_collection()
        f.index()
