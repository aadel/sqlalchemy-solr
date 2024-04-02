import logging

import sqlparse
from pandas import to_datetime
from sqlparse.sql import Identifier
from sqlparse.sql import IdentifierList
from sqlparse.tokens import Keyword

from .. import type_map
from ..api_globals import _HEADER
from ..message_formatter import MessageFormatter


class SolrTableReflection:
    mf = MessageFormatter()
    connection = None
    table_metadata_cache = {}

    @staticmethod
    def reflect_column_types(df, operation):
        tables = list(
            map(
                SolrTableReflection.unquote,
                SolrTableReflection.extract_tables(operation),
            )
        )
        names, types = [], []

        for table in tables:
            if table not in SolrTableReflection.table_metadata_cache:
                session = SolrTableReflection.connection.session

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
                    params={"wt": "json"},
                    headers=_HEADER,
                )
                fields = result.json()["fields"]

                SolrTableReflection.table_metadata_cache[table] = fields

            for column in df:
                names.append(column)
                # Search for column type in cache
                prototype = None
                for table in tables:
                    try:
                        if column in SolrTableReflection.table_metadata_cache[table]:
                            prototype = type_map.type_map[
                                SolrTableReflection.table_metadata_cache[table][column][
                                    "type"
                                ]
                            ]
                    except (AttributeError, KeyError) as e:
                        logging.warning(e)

                    if not prototype:
                        prototype = SolrTableReflection.infer_column_type(df, column)

                    types.append(prototype)

        return names, types

    @staticmethod
    def infer_column_type(df, column):
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
                    logging.warning(
                        "Cannot infer type of column %s, defaulting to varchar", column
                    )
                    return "varchar"

    @staticmethod
    def extract_from_part(parsed):
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if item.ttype is Keyword:
                    return
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
            return None
        if val[0] in ('"', "'", "`") and val[0] == val[-1]:
            val = val[1:-1]
        return val
