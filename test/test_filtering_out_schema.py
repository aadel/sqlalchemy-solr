import logging

from sqlalchemy import and_
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table

from .fixtures.fixtures import SalesFixture


class TestFilteringOutSchema:
    def index_data(self, settings):
        f = SalesFixture(settings["SOLR_BASE_URL"])
        f.truncate_collection()
        f.index()

    def test_solr_filtering_out_schema(self, settings):
        self.index_data(settings)
        metadata = MetaData()
        engine = create_engine(
            settings["SOLR_CONNECTION_URI"]
            + "/"
            + settings["SOLR_WORKER_COLLECTION_NAME"],
            echo=True,
        )
        t = Table(
            settings["SOLR_WORKER_COLLECTION_NAME"],
            metadata,
            autoload=True,
            autoload_with=engine,
        )

        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nFROM `default`.sales_test_ \nWHERE TRUE"
        SELECT_CLAUSE_2 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_\n LIMIT ?"

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
        )

        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE TRUE"
        SELECT_CLAUSE_2 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_\n LIMIT ?"

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
        )

        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nFROM default.sales_test_ \nWHERE TRUE"
        SELECT_CLAUSE_2 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_\n LIMIT ?"

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
        )

        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nFROM 'public'.sales_test_ \nWHERE TRUE"
        SELECT_CLAUSE_2 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_\n LIMIT ?"

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
        )

        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nfrom sales_test_ \nWHERE TRUE"
        SELECT_CLAUSE_2 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_\n LIMIT ?"

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
        )