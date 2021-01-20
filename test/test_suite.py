from sqlalchemy import create_engine
from sqlalchemy import select, and_
from sqlalchemy import Table, MetaData

from .fixtures.fixtures import SalesFixture
from .test_db_creation import DBCreationTest

class TestSuite:
    SOLR_BASE_URL = 'http://solr:8983/solr'
    
    def test_indexing_data(self):
        f = SalesFixture(TestSuite.SOLR_BASE_URL)
        f.truncate_collection()
        f.index()

    def test_db_creation(self):
        DBCreationTest()

    def test_solr_compiler(self):
        metadata = MetaData()
        engine = create_engine('solr://solr:8983/solr/sales_test_', echo=True)
        t = Table('sales_test_', metadata, autoload=True, autoload_with=engine)
        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)             # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] >= '2017-05-10 00:00:00',    # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] <= '2017-05-20 00:00:00'))                    # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.rowcount == 18