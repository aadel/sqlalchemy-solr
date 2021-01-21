from sqlalchemy import create_engine
from sqlalchemy import select, and_
from sqlalchemy import Table, MetaData

from sqlalchemy_solr.http import SolrDialect_http

from .fixtures.fixtures import SalesFixture
from .test_db_creation import DBCreationTest

class TestSuite:
    SOLR_BASE_URL = 'http://solr:8983/solr'
    SOLR_CONNECTION_URI = 'solr://solr:8983/solr'
    COLLECTION_NAME = 'sales_test_'

    def test_indexing_data(self):
        f = SalesFixture(TestSuite.SOLR_BASE_URL)
        f.truncate_collection()
        f.index()

    def test_db_creation(self):
        DBCreationTest()

    def test_solr_date_compilation(self):
        metadata = MetaData()
        engine = create_engine(TestSuite.SOLR_CONNECTION_URI + '/'
            + TestSuite.COLLECTION_NAME, echo=True)
        t = Table(self.COLLECTION_NAME, metadata, autoload=True, autoload_with=engine)

        lower_bound = '2017-05-10 00:00:00'
        upper_bound = '2017-05-20 00:00:00'
        lower_bound_iso = '2017-05-10T00:00:00Z'
        upper_bound_iso = '2017-05-20T00:00:00Z'
        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE TRUE "
        SELECT_CLAUSE_2 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE sales_test_.`ORDERDATE_dt` = "

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] >= lower_bound,      # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] <= upper_bound))                      # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_1 \
                + "AND sales_test_.`ORDERDATE_dt` = '[" + lower_bound_iso \
                + " TO " + upper_bound_iso + "]'\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] > lower_bound,       # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] <= upper_bound))                      # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_1 \
                + "AND sales_test_.`ORDERDATE_dt` = '{" + lower_bound_iso \
                + " TO " + upper_bound_iso + "]'\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] >= lower_bound,      # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] < upper_bound))                       # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_1 \
                + "AND sales_test_.`ORDERDATE_dt` = '[" + lower_bound_iso \
                + " TO " + upper_bound_iso + "}'\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] > lower_bound,       # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] < upper_bound))                       # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_1 \
                + "AND sales_test_.`ORDERDATE_dt` = '{" + lower_bound_iso \
                + " TO " + upper_bound_iso + "}'\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] <= upper_bound,      # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] >= lower_bound))                      # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_2 + "'[" + lower_bound_iso + " TO " \
                + upper_bound_iso + "]' AND TRUE\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] < upper_bound,       # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] >= lower_bound))                      # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_2 + "'[" + lower_bound_iso + " TO " \
                + upper_bound_iso + "}' AND TRUE\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] <= upper_bound,      # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] > lower_bound))                       # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_2 + "'{" + lower_bound_iso + " TO " \
                + upper_bound_iso + "]' AND TRUE\n LIMIT ?"

        qry = (select([t.columns['CITY_s']]).select_from(t)).limit(100)     # pylint: disable=unsubscriptable-object
        qry = qry.where(and_(t.columns['ORDERDATE_dt'] < upper_bound,       # pylint: disable=unsubscriptable-object
            t.columns['ORDERDATE_dt'] > lower_bound))                       # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert result.context.statement == \
            SELECT_CLAUSE_2 + "'{" + lower_bound_iso + " TO " \
                + upper_bound_iso + "}' AND TRUE\n LIMIT ?"
