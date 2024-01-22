from sqlalchemy import and_
from sqlalchemy import select

from tests.setup import prepare_orm

class TestDateRangeCompilation:

    def test_solr_date_range_compilation(self, settings):
        engine, t = prepare_orm(settings)

        lower_bound = "2017-05-10 00:00:00"
        upper_bound = "2017-05-20 00:00:00"
        lower_bound_iso = "2017-05-10T00:00:00Z"
        upper_bound_iso = "2017-05-20T00:00:00Z"
        select_statement_1 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE TRUE "
        select_statement_2 = (
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
            == select_statement_1
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
            == select_statement_1
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
            == select_statement_1
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
            == select_statement_1
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
            == select_statement_2
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
            == select_statement_2
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
            == select_statement_2
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
            == select_statement_2
            + "'{"
            + lower_bound_iso
            + " TO "
            + upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )
