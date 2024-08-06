from sqlalchemy import select
from tests.setup import prepare_orm


class TestOperatorTranslation:

    def test_ilike_operator_translation(self, settings):
        engine, t = prepare_orm(settings)

        select_statement_1 = (
            "SELECT sales_test_.`CITY_s` \nFROM sales_test_ "
            "\nWHERE sales_test_.`CITY_s` LIKE ?\n LIMIT ?"
        )

        qry = (select(t.c.CITY_s).where(t.c.CITY_s.ilike("%york%"))).limit(
            1
        )  # pylint: disable=unsubscriptable-object

        with engine.connect() as connection:
            result = connection.execute(qry)

        assert result.context.statement == select_statement_1

    def test_not_ilike_operator_translation(self, settings):
        engine, t = prepare_orm(settings)

        select_statement_1 = (
            "SELECT sales_test_.`CITY_s` \nFROM sales_test_ "
            "\nWHERE sales_test_.`CITY_s` NOT LIKE ?\n LIMIT ?"
        )

        qry = (select(t.c.CITY_s).where(t.c.CITY_s.notilike("%york%"))).limit(
            1
        )  # pylint: disable=unsubscriptable-object

        with engine.connect() as connection:
            result = connection.execute(qry)

        assert result.context.statement == select_statement_1
