from sqlalchemy import select
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.util.langhelpers import _symbol
from tests.setup import prepare_orm

from .fixtures.fixtures import SalesFixture


class TestSQLCompilationCaching:
    def index_data(self, settings):
        f = SalesFixture(settings)
        f.truncate_collection()
        f.index()

    def test_sql_compilation_caching_1(self, settings):
        _, t = prepare_orm(settings)

        qry_1 = (select(t.c.CITY_s).select_from(t)).limit(1)
        qry_2 = (select(t.c.CITY_s).select_from(t)).limit(10)

        k1 = qry_1._generate_cache_key()  # pylint: disable=protected-access
        k2 = qry_2._generate_cache_key()  # pylint: disable=protected-access

        assert k1 == k2

    def test_sql_compilation_caching_2(self, settings):
        _, t = prepare_orm(settings)

        qry_1 = (select(t.c.CITY_s).select_from(t)).limit(1).offset(1)
        qry_2 = (select(t.c.CITY_s).select_from(t)).limit(1).offset(2)

        k1 = qry_1._generate_cache_key()  # pylint: disable=protected-access
        k2 = qry_2._generate_cache_key()  # pylint: disable=protected-access

        assert k1 == k2

    def test_sql_compilation_caching_3(self, settings):
        engine, t = prepare_orm(settings)

        qry = select(t).where(t.c.CITY_s == bindparam("CITY_s")).limit(10)

        with engine.connect() as connection:
            result_1 = connection.execute(qry, {"CITY_s": "Singapore"})
            result_2 = connection.execute(qry, {"CITY_s": "Boras"})

        assert result_1.context.cache_hit == _symbol("CACHE_MISS")
        assert result_2.context.cache_hit == _symbol("CACHE_HIT")

    def test_sql_compilation_caching_4(self, settings):
        _, t = prepare_orm(settings)

        qry_1 = select(t).where(t.c.CITY_s == bindparam("CITY_s")).limit(10)
        qry_2 = select(t).where(t.c.COUNTRY_s == bindparam("COUNTRY_s")).limit(10)

        k1 = qry_1._generate_cache_key()  # pylint: disable=protected-access
        k2 = qry_2._generate_cache_key()  # pylint: disable=protected-access

        assert k1 != k2
