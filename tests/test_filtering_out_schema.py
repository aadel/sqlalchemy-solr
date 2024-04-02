from sqlalchemy import select
from tests.setup import prepare_orm

from .fixtures.fixtures import SalesFixture


class TestFilteringOutSchema:
    def index_data(self, settings):
        f = SalesFixture(settings)
        f.truncate_collection()
        f.index()

    def test_solr_filtering_out_schema(self, settings):
        engine, t = prepare_orm(settings)

        t.schema = "default"

        select_statement_1 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_\n LIMIT ?"

        qry = (select(t.c.CITY_s).select_from(t)).limit(
            1
        )  # pylint: disable=unsubscriptable-object

        with engine.connect() as connection:
            result = connection.execute(qry)

        assert result.context.statement == select_statement_1
