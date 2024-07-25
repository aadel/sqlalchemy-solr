from sqlalchemy import select
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.util.langhelpers import _symbol
from tests import assertions
from tests.setup import prepare_orm

releases = [6, 7, 8]


class TestSQLCompilationCaching:

    def test_sql_compilation_caching_1(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry_1 = (select(t.c.COUNTRY_s).select_from(t)).limit(1)
        qry_2 = (select(t.c.COUNTRY_s).select_from(t)).limit(10)

        with engine.connect() as connection:
            result_1 = connection.execute(qry_1)
            result_2 = connection.execute(qry_2)

        assert result_1.context.cache_hit == _symbol("NO_DIALECT_SUPPORT")
        assert result_2.context.cache_hit == _symbol("NO_DIALECT_SUPPORT")

    def test_sql_compilation_caching_2(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry_1 = (select(t.c.COUNTRY_s).select_from(t)).limit(1).offset(1)
        qry_2 = (select(t.c.COUNTRY_s).select_from(t)).limit(1).offset(2)

        with engine.connect() as connection:
            result_1 = connection.execute(qry_1)
            result_2 = connection.execute(qry_2)

        assert result_1.context.cache_hit == _symbol("NO_DIALECT_SUPPORT")
        assert result_2.context.cache_hit == _symbol("NO_DIALECT_SUPPORT")

    def test_sql_compilation_caching_3(self, settings):
        assertions.assert_solr_release(settings, releases)

        engine, t = prepare_orm(settings)

        qry = select(t).where(t.c.COUNTRY_s == bindparam("COUNTRY_s")).limit(10)

        with engine.connect() as connection:
            result_1 = connection.execute(qry, {"COUNTRY_s": "Sweden"})
            result_2 = connection.execute(qry, {"COUNTRY_s": "France"})

        assert result_1.context.cache_hit == _symbol("NO_DIALECT_SUPPORT")
        assert result_2.context.cache_hit == _symbol("NO_DIALECT_SUPPORT")
