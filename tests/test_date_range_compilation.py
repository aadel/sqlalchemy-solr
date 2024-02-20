from dataclasses import dataclass
import pytest
from sqlalchemy import and_
from sqlalchemy import select
from sqlalchemy import Table

from tests.setup import prepare_orm


@dataclass
class Parameters:
    engine: any
    t: Table
    lower_bound: str
    upper_bound: str
    lower_bound_iso: str
    upper_bound_iso: str
    select_statements: list[str]


class TestDateRangeCompilation:

    @pytest.fixture(scope="class")
    def parameters(self, settings):
        engine, t = prepare_orm(settings)

        lower_bound = "2017-05-10 00:00:00"
        upper_bound = "2017-05-20 00:00:00"
        lower_bound_iso = "2017-05-10T00:00:00Z"
        upper_bound_iso = "2017-05-20T00:00:00Z"
        select_statement_1 = (
            "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE TRUE "
        )
        select_statement_2 = (
            "SELECT sales_test_.`CITY_s` \nFROM sales_test_ "
            "\nWHERE sales_test_.`ORDERDATE_dt` = "
        )

        p = Parameters(
            engine,
            t,
            lower_bound,
            upper_bound,
            lower_bound_iso,
            upper_bound_iso,
            [select_statement_1, select_statement_2],
        )

        return p

    def test_solr_date_range_compilation_1(self, parameters):

        p = parameters

        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    >= p.lower_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] <= p.upper_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '["
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_2(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    > p.lower_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] <= p.upper_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '{"
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_3(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    >= p.lower_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] < p.upper_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '["
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "}'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_4(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    > p.lower_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] < p.upper_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '{"
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "}'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_5(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    <= p.upper_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] >= p.lower_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'["
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "]' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_6(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    < p.upper_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] >= p.lower_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'["
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_7(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    <= p.upper_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] > p.lower_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'{"
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "]' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_8(self, parameters):
        p = parameters
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(
                and_(
                    p.t.columns["ORDERDATE_dt"]
                    < p.upper_bound,  # pylint: disable=unsubscriptable-object
                    p.t.columns["ORDERDATE_dt"] > p.lower_bound,
                )
            )
            .limit(1)
        )  # pylint: disable=unsubscriptable-object

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'{"
            + p.lower_bound_iso
            + " TO "
            + p.upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_9(self, parameters):
        p = parameters
        # pylint: disable=unsubscriptable-object
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(p.t.columns["ORDERDATE_dt"] > p.lower_bound)
            .limit(1)
        )

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'{"
            + p.lower_bound_iso
            + " TO "
            + "*"
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_10(self, parameters):
        p = parameters
        # pylint: disable=unsubscriptable-object
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(p.t.columns["ORDERDATE_dt"] <= p.upper_bound)
            .limit(1)
        )

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'["
            + "*"
            + " TO "
            + p.upper_bound_iso
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_11(self, parameters):
        p = parameters
        # pylint: disable=unsubscriptable-object
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(p.t.columns["ORDERDATE_dt"] >= p.lower_bound)
            .limit(1)
        )

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'["
            + p.lower_bound_iso
            + " TO "
            + "*"
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_12(self, parameters):
        p = parameters
        # pylint: disable=unsubscriptable-object
        qry = (
            (select(p.t.c.CITY_s).select_from(p.t))
            .where(p.t.columns["ORDERDATE_dt"] < p.upper_bound)
            .limit(1)
        )

        with p.engine.connect() as connection:
            result = connection.execute(qry)

        assert (
            result.context.statement
            == p.select_statements[1]
            + "'["
            + "*"
            + " TO "
            + p.upper_bound_iso
            + "}'\n LIMIT ?"
        )
