from sqlalchemy import select
from sqlalchemy.sql.expression import bindparam

from tests.setup import prepare_orm
from .fixtures.fixtures import SalesFixture

class TestSQLCompilationCaching:
    def index_data(self, settings):
        f = SalesFixture(settings)
        f.truncate_collection()
        f.index()

    def test_sql_compilation_caching_1(self, caplog, settings):
        engine, t = prepare_orm(settings)

        qry_1 = (select(t.c.CITY_s).select_from(t)).limit(1)  # pylint: disable=unsubscriptable-object
        qry_2 = (select(t.c.CITY_s).select_from(t)).limit(10)  # pylint: disable=unsubscriptable-object

        with engine.connect() as connection:
            connection.execute(qry_1)
            connection.execute(qry_2)

        assert 'generated in' in caplog.text
        assert 'cached since' in caplog.text

    def test_sql_compilation_caching_2(self, caplog, settings):
        engine, t = prepare_orm(settings)

        qry_1 = (select(t.c.CITY_s).select_from(t)).limit(1).offset(1)  # pylint: disable=unsubscriptable-object
        qry_2 = (select(t.c.CITY_s).select_from(t)).limit(1).offset(2)  # pylint: disable=unsubscriptable-object

        with engine.connect() as connection:
            connection.execute(qry_1)
            connection.execute(qry_2)

        assert 'generated in' in caplog.text
        assert 'cached since' in caplog.text

    def test_sql_compilation_caching_3(self, caplog, settings):
        engine, t = prepare_orm(settings)

        qry = select(t).where(t.c.CITY_s == bindparam('CITY_s')).limit(10)  # pylint: disable=unsubscriptable-object

        with engine.connect() as connection:
            connection.execute(qry, params={'CITY_s': 'Singapore'})
            connection.execute(qry, CITY_s='Boras')

        assert 'generated in' in caplog.text
        assert 'cached since' in caplog.text
