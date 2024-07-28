from requests import Session
from requests.auth import HTTPBasicAuth
from sqlalchemy import types
from sqlalchemy_solr.solrdbapi import Connection
from sqlalchemy_solr.solrdbapi import Cursor
from sqlalchemy_solr.solrdbapi import SolrTableReflection


class TestSolrTableReflection:
    # pylint: disable=too-few-public-methods
    def test_solr_columns_reflection(self, settings):
        sql = """
        SELECT CITY_s, PHONE_ss FROM sales_test_ LIMIT 10
        """

        if settings["SOLR_USER"]:
            auth = HTTPBasicAuth(settings["SOLR_USER"], settings["SOLR_PASS"])
        else:
            auth = None

        session = Session()
        session.auth = auth

        SolrTableReflection.connection = Connection(
            settings["HOST"],
            "test",
            settings["SOLR_USER"],
            settings["SOLR_PASS"],
            settings["SERVER_PATH"],
            settings["SOLR_WORKER_COLLECTION_NAME"],
            settings["PORT"],
            settings["PROTO"],
            session,
        )

        cursor = Cursor(
            settings["HOST"],
            f'{settings["SERVER_PATH"]}/{settings["SOLR_WORKER_COLLECTION_NAME"]}',
            settings["SOLR_USER"],
            settings["SOLR_PASS"],
            settings["SERVER_PATH"],
            settings["SOLR_WORKER_COLLECTION_NAME"],
            settings["PORT"],
            settings["PROTO"],
            session,
            SolrTableReflection.connection,
        )
        result_set = cursor.execute(sql, [])

        assert result_set.rowcount > 0

        columns = map(lambda col: col[0], result_set.description)

        reflected_data_types = SolrTableReflection.reflect_column_types(
            result_set, columns, sql
        )

        reflected_columns = dict(
            zip(reflected_data_types[0], reflected_data_types[1])  # noqa: B905
        )

        assert isinstance(reflected_columns["CITY_s"], types.VARCHAR)
        assert isinstance(reflected_columns["PHONE_ss"], types.ARRAY)
        assert isinstance(reflected_columns["PHONE_ss"].item_type, types.VARCHAR)
