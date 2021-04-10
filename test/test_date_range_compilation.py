from sqlalchemy import and_
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table

from .fixtures.fixtures import SalesFixture


class TestDateRangeCompilation:
    def index_data(self, settings):
        f = SalesFixture(settings["SOLR_BASE_URL"])
        f.truncate_collection()
        f.index()

    def test_solr_date_range_compilation(self, settings):
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

        lower_bound = "2017-05-10 00:00:00"
        upper_bound = "2017-05-20 00:00:00"
        lower_bound_iso = "2017-05-10T00:00:00Z"
        upper_bound_iso = "2017-05-20T00:00:00Z"
        SELECT_CLAUSE_1 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE TRUE "
        SELECT_CLAUSE_2 = (
            "SELECT sales_test_.`CITY_s` \nFROM sales_test_ "
            "\nWHERE sales_test_.`ORDERDATE_dt` = "
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                >= lower_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] <= upper_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_1
            + "AND sales_test_.`ORDERDATE_dt` = '["
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "]'\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                > lower_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] <= upper_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_1
            + "AND sales_test_.`ORDERDATE_dt` = '{"
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "]'\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                >= lower_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] < upper_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_1
            + "AND sales_test_.`ORDERDATE_dt` = '["
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "}'\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                > lower_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] < upper_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_1
            + "AND sales_test_.`ORDERDATE_dt` = '{"
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "}'\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                <= upper_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] >= lower_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
            + "'["
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "]' AND TRUE\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                < upper_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] >= lower_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
            + "'["
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                <= upper_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] > lower_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
            + "'{"
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "]' AND TRUE\n LIMIT ?"
        )

        qry = (select([t.columns["CITY_s"]]).select_from(t)).limit(
            100
        )  # pylint: disable=unsubscriptable-object
        qry = qry.where(
            and_(
                t.columns["ORDERDATE_dt"]
                < upper_bound,  # pylint: disable=unsubscriptable-object
                t.columns["ORDERDATE_dt"] > lower_bound,
            )
        )  # pylint: disable=unsubscriptable-object

        result = engine.execute(qry)

        assert (
            result.context.statement
            == SELECT_CLAUSE_2
            + "'{"
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )
