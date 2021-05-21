import logging

import sqlparse
from pandas import to_datetime
from requests import Session
from sqlalchemy_solr import base
from sqlalchemy_solr.api_globals import _HEADER
from sqlalchemy_solr.message_formatter import MessageFormatter
from sqlparse.sql import Identifier
from sqlparse.sql import IdentifierList
from sqlparse.tokens import Keyword


class SolrTableReflection:
    mf = MessageFormatter()
    connection = None
    table_metadata_cache = {}

    @staticmethod
    def reflect_column_types(df, operation):
        tables = list(
            map(
                lambda t: SolrTableReflection.unquote(t),
                SolrTableReflection.extract_tables(operation),
            )
        )
        names, types = [], []

        for table in tables:
            if table not in SolrTableReflection.table_metadata_cache:
                session = Session()

                result = session.get(
                    SolrTableReflection.connection.proto
                    + SolrTableReflection.connection.host
                    + ":"
                    + str(SolrTableReflection.connection.port)
                    + "/"
                    + SolrTableReflection.connection.server_path
                    + "/"
                    + table
                    + "/admin/luke",
                    headers=_HEADER,
                    auth=(
                        SolrTableReflection.connection.username,
                        SolrTableReflection.connection.password,
                    ),
                )
                fields = result.json()["fields"]

                SolrTableReflection.table_metadata_cache[table] = fields

        try:
            for column in df:
                names.append(column)
                # Search for column type in cache
                prototype = None
                for table in tables:
                    if column in SolrTableReflection.table_metadata_cache[table]:
                        prototype = base._type_map[
                            SolrTableReflection.table_metadata_cache[table][column][
                                "type"
                            ]
                        ]
                if not prototype:
                    prototype = SolrTableReflection.infer_column_type(df, column)
                types.append(prototype)

        except Exception:
            logging.exception(
                SolrTableReflection.mf.format(
                    "Error in SolrReflect.reflect_column_types"
                )
            )
        return names, types

    @staticmethod
    def infer_column_type(df, column):
        try:
            try:
                df[column] = df[column].astype(int)
                return "bigint"
            except ValueError:
                try:
                    df[column] = df[column].astype(float)
                    return "decimal"
                except ValueError:
                    try:
                        df[column] = to_datetime(df[column])
                        return "timestamp"
                    except ValueError:
                        return "varchar"
        except Exception:
            logging.exception(
                SolrTableReflection.mf.format(
                    "Error in SolrTableReflection.infer_column_types"
                )
            )
        return type

    @staticmethod
    def extract_from_part(parsed):
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if item.ttype is Keyword:
                    return
                else:
                    yield item
            elif item.ttype is Keyword and item.value.upper() == "FROM":
                from_seen = True

    @staticmethod
    def extract_table_identifiers(token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_name()
            elif isinstance(item, Identifier):
                yield item.get_name()
            # It's a bug to check for Keyword here, check sqlparse documentation
            elif item.ttype is Keyword:
                yield item.value

    @staticmethod
    def extract_tables(sql):
        stream = SolrTableReflection.extract_from_part(sqlparse.parse(sql)[0])
        return list(SolrTableReflection.extract_table_identifiers(stream))

    @staticmethod
    def unquote(val):
        """Unquote labels."""
        if val is None:
            return
        if val[0] in ('"', "'", "`") and val[0] == val[-1]:
            val = val[1:-1]
        return val
