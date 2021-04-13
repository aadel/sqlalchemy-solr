from numpy import nan
from pandas import DataFrame
from requests import Session
from sqlalchemy import types
from sqlalchemy_solr.solrdbapi import Connection
from sqlalchemy_solr.solrdbapi import Cursor
from sqlalchemy_solr.solrdbapi import SolrTableReflection


class TestSolrTableReflection:
    def test_solr_columns_reflection(self, settings):
        sql = """
        SELECT CITY_s, PHONE_ss FROM sales_test_
        """
        session = Session()

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
        result = Cursor.submit_query(
            Cursor.substitute_in_query(sql, []),
            settings["HOST"],
            settings["PORT"],
            settings["PROTO"],
            settings["SOLR_USER"],
            settings["SOLR_PASS"],
            settings["SERVER_PATH"],
            settings["SOLR_WORKER_COLLECTION_NAME"],
            session,
        )

        rows = result.json()["result-set"]["docs"]

        assert len(rows) > 0

        columns = rows[0].keys()

        _resultSet = DataFrame(data=rows, columns=columns).fillna(value=nan)

        reflected_data_types = SolrTableReflection.reflect_column_types(_resultSet, sql)

        columns = {
            k: v for (k, v) in zip(reflected_data_types[0], reflected_data_types[1])
        }

        assert isinstance(columns["CITY_s"], types.VARCHAR)
        assert isinstance(columns["PHONE_ss"], types.ARRAY)
        assert isinstance(columns["PHONE_ss"].item_type, types.VARCHAR)
