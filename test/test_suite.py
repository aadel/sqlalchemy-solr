from .fixtures.fixtures import SalesFixture
from .test_db_creation import DBCreationTest

class TestSuite:
    BASE_URL = 'http://solr:8983/solr'
    
    def test_indexing_data(self):
        f = SalesFixture(TestSuite.BASE_URL)
        f.truncate_collection()
        f.index()

    def test_db_creation(self):
        DBCreationTest()
