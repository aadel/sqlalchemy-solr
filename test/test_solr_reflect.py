from numpy import nan
from pandas import DataFrame
from requests import Session
from sqlalchemy import types
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import sqlparse
import logging

from sqlalchemy_solr.message_formatter import MessageFormatter
from sqlalchemy_solr.solrdbapi import SolrTableReflection, Cursor, Connection


class TestSolrTableReflection:

    def extract_from_part(self, parsed):
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if item.ttype is Keyword:
                    return
                else:
                    yield item
            elif item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True

    def extract_table_identifiers(self, token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_name()
            elif isinstance(item, Identifier):
                yield item.get_name()
            # It's a bug to check for Keyword here, but in the example
            # above some tables names are identified as keywords...
            elif item.ttype is Keyword:
                yield item.value

    def extract_tables(self, sql):
        stream = self.extract_from_part(sqlparse.parse(sql)[0])
        return list(self.extract_table_identifiers(stream))

    def test_solr_reflect(self, settings):
        sql = """
        SELECT 1 FROM (VALUES(1))
        """
        session = Session()
        mf = MessageFormatter()

        SolrTableReflection.connection = Connection(settings['HOST'], 'test',
                                                    settings['SOLR_USER'],
                                                    settings['SOLR_PASS'],
                                                    settings['SERVER_PATH'],
                                                    settings['SOLR_WORKER_COLLECTION_NAME'],
                                                    settings['PORT'],
                                                    settings['PROTO'],
                                                    session)
        result = Cursor.submit_query(
            Cursor.substitute_in_query(sql, []),
            settings['HOST'],
            settings['PORT'],
            settings['PROTO'],
            settings['SOLR_USER'],
            settings['SOLR_PASS'],
            settings['SERVER_PATH'],
            settings['SOLR_WORKER_COLLECTION_NAME'],
            session
        )

        rows = result.json()["result-set"]['docs']

        assert len(rows) > 0

        columns = rows[0].keys()

        _resultSet = (DataFrame(data=rows, columns=columns).fillna(value=nan))

        logging.info(mf.format("Query:", sql))

        tables = ', '.join(self.extract_tables(sql))

        # assert settings['SOLR_WORKER_COLLECTION_NAME'] in tables
        reflected_data_types = SolrTableReflection.reflect_column_types(_resultSet, sql)
        assert (["CITY_s"], [types.VARCHAR]) == reflected_data_types
