from sqlalchemy import and_
from sqlalchemy import select
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.util.langhelpers import _symbol
from tests import assertions
from tests.setup import prepare_orm

releases = [9]


class TestSQLCompilationCaching:

    def test_sql_compilation_caching_1(self, settings):
        assertions.assert_solr_release(settings, releases)

        _, t = prepare_orm(settings)

        qry_1 = (select(t.c.CITY_s).select_from(t)).limit(1)
        qry_2 = (select(t.c.CITY_s).select_from(t)).limit(10)

        k1 = qry_1._generate_cache_key()  # pylint: disable=protected-access
        k2 = qry_2._generate_cache_key()  # pylint: disable=protected-access

        assert k1 == k2

    def test_sql_compilation_caching_2(self, settings):
        assertions.assert_solr_release(settings, releases)

        _, t = prepare_orm(settings)

        qry_1 = (select(t.c.CITY_s).select_from(t)).limit(1).offset(1)
        qry_2 = (select(t.c.CITY_s).select_from(t)).limit(1).offset(2)

        k1 = qry_1._generate_cache_key()  # pylint: disable=protected-access
        k2 = qry_2._generate_cache_key()  # pylint: disable=protected-access

        assert k1 == k2

    def test_sql_compilation_caching_3(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry = select(t).where(t.c.CITY_s == bindparam("CITY_s")).limit(10)

        with engine.connect() as connection:
            result_1 = connection.execute(qry, {"CITY_s": "Singapore"})
            result_2 = connection.execute(qry, {"CITY_s": "Boras"})

        assert result_1.context.cache_hit == _symbol("CACHE_MISS")
        assert result_2.context.cache_hit == _symbol("CACHE_HIT")

    def test_sql_compilation_caching_4(self, settings):
        assertions.assert_solr_release(settings, releases)

        _, t = prepare_orm(settings)

        qry_1 = select(t).where(t.c.CITY_s == bindparam("CITY_s")).limit(10)
        qry_2 = select(t).where(t.c.COUNTRY_s == bindparam("COUNTRY_s")).limit(10)

        k1 = qry_1._generate_cache_key()  # pylint: disable=protected-access
        k2 = qry_2._generate_cache_key()  # pylint: disable=protected-access

        assert k1 != k2

    def test_sql_compilation_caching_5(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry = select(t).where(t.c.ORDERDATE_dt >= bindparam("ORDERDATE_dt")).limit(10)

        with engine.connect() as connection:
            result_1 = connection.execute(qry, {"ORDERDATE_dt": "2018-01-01T00:00:00Z"})
            result_2 = connection.execute(qry, {"ORDERDATE_dt": "2018-01-01T00:00:00Z"})

        assert result_1.context.cache_hit == _symbol("CACHE_MISS")
        assert result_2.context.cache_hit == _symbol("CACHE_HIT")

    def test_sql_compilation_caching_6(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry = (
            select(t.c.ORDERNUMBER_i, t.c.ORDERLINENUMBER_i)
            .where(t.c.ORDERDATE_dt >= bindparam("ORDERDATE_dt"))
            .order_by(t.c.ORDERDATE_dt.asc())
            .limit(10)
        )

        with engine.connect() as connection:
            result_1 = connection.execute(qry, {"ORDERDATE_dt": "2017-05-01T00:00:00Z"})
            result_2 = connection.execute(qry, {"ORDERDATE_dt": "2017-06-01T00:00:00Z"})

        for row in zip(result_1, result_2, strict=False):
            assert row[0][0] != row[1][0]

        assert result_1.context.cache_hit == _symbol("CACHE_MISS")
        assert result_2.context.cache_hit == _symbol("CACHE_HIT")

    def test_sql_compilation_caching_7(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry = (
            select(t)
            .where(
                and_(
                    t.c.ORDERDATE_dt
                    >= bindparam(
                        "ORDERDATE_dt_1",
                        t.c.ORDERDATE_dt <= bindparam("ORDERDATE_dt_2"),
                    )
                )
            )
            .limit(10)
        )

        with engine.connect() as connection:
            result_1 = connection.execute(
                qry,
                {
                    "ORDERDATE_dt_1": "2017-07-01T00:00:00Z",
                    "ORDERDATE_dt_2": "2017-08-01T00:00:00Z",
                },
            )
            result_2 = connection.execute(
                qry,
                {
                    "ORDERDATE_dt_1": "2017-07-01T00:00:00Z",
                    "ORDERDATE_dt_2": "2017-08-01T00:00:00Z",
                },
            )

        assert result_1.context.cache_hit == _symbol("CACHE_MISS")
        assert result_2.context.cache_hit == _symbol("CACHE_HIT")

    def test_sql_compilation_caching_8(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry = (
            select(t)
            .where(
                and_(
                    t.c.ORDERDATE_dt
                    >= bindparam(
                        "ORDERDATE_dt_1",
                        t.c.ORDERDATE_dt <= bindparam("ORDERDATE_dt_2"),
                    )
                )
            )
            .limit(10)
        )

        with engine.connect() as connection:
            result_1 = connection.execute(
                qry,
                {
                    "ORDERDATE_dt_1": "2017-09-01T00:00:00Z",
                    "ORDERDATE_dt_2": "2017-10-01T00:00:00Z",
                },
            )
            result_2 = connection.execute(
                qry,
                {
                    "ORDERDATE_dt_1": "2017-11-01T00:00:00Z",
                    "ORDERDATE_dt_2": "2017-12-01T00:00:00Z",
                },
            )

        for row in zip(result_1, result_2, strict=False):
            assert row[0][0] != row[1][0]

        assert result_1.context.cache_hit == _symbol("CACHE_MISS")
        assert result_2.context.cache_hit == _symbol("CACHE_HIT")
