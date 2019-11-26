# This is the MIT license: http://www.opensource.org/licenses/mit-license.php
#
# Copyright (c) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>.
# SQLAlchemy is a trademark of Michael Bayer.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import exc, pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import inspect
from requests import Session
from sqlalchemy_solr.solrdbapi import api_globals
import re
import logging

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.ERROR)

_type_map = {
    'bit': types.BOOLEAN,
    'bigint': types.BIGINT,
    'binary': types.LargeBinary,
    'boolean': types.BOOLEAN,
    'date': types.DATE,
    'decimal': types.DECIMAL,
    'double': types.FLOAT,
    'int': types.INTEGER,
    'integer': types.INTEGER,
    'interval': types.Interval,
    'smallint': types.SMALLINT,
    'timestamp': types.TIMESTAMP,
    'time': types.TIME,
    'varchar': types.String,
    'character varying': types.String,
    'any': types.String,
    'map': types.UserDefinedType,
    'list': types.UserDefinedType,
    'float8': types.FLOAT,
}

class SolrCompiler(compiler.SQLCompiler):

    def default_from(self):
        raise NotImplementedError


class SolrIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(
        [
            'abs', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array', 'as', 'asensitive',
            'asymmetric', 'at', 'atomic', 'authorization', 'avg', 'begin', 'between', 'bigint', 'binary',
            'bit', 'blob', 'boolean', 'both', 'by', 'call', 'called', 'cardinality', 'cascaded', 'case',
            'cast', 'ceil', 'ceiling', 'char', 'character', 'character_length', 'char_length', 'check',
            'clob', 'close', 'coalesce', 'collate', 'collect', 'column', 'commit', 'condition', 'connect',
            'constraint', 'convert', 'corr', 'corresponding', 'count', 'covar_pop', 'covar_samp', 'create',
            'cross', 'cube', 'cume_dist', 'current', 'current_catalog', 'current_date',
            'current_default_transform_group', 'current_path', 'current_role', 'current_schema', 'current_time',
            'current_timestamp', 'current_transform_group_for_type', 'current_user', 'cursor', 'cycle',
            'databases', 'date', 'day', 'deallocate', 'dec', 'decimal', 'declare', 'default', 'default_kw',
            'delete', 'dense_rank', 'deref', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct',
            'double', 'drop', 'dynamic', 'each', 'element', 'else', 'end', 'end_exec', 'escape', 'every', 'except',
            'exec', 'execute', 'exists', 'exp', 'explain', 'external', 'extract', 'false', 'fetch', 'files', 'filter',
            'first_value', 'float', 'floor', 'for', 'foreign', 'free', 'from', 'full', 'function', 'fusion', 'get',
            'global', 'grant', 'group', 'grouping', 'having', 'hold', 'hour', 'identity', 'if', 'import', 'in',
            'indicator', 'inner', 'inout', 'insensitive', 'insert', 'int', 'integer', 'intersect', 'intersection',
            'interval', 'into', 'is', 'jar', 'join', 'language', 'large', 'last_value', 'lateral', 'leading', 'left',
            'like', 'limit', 'ln', 'local', 'localtime', 'localtimestamp', 'lower', 'match', 'max', 'member', 'merge',
            'method', 'min', 'minute', 'mod', 'modifies', 'module', 'month', 'multiset', 'national', 'natural',
            'nchar', 'nclob', 'new', 'no', 'none', 'normalize', 'not', 'null', 'nullif', 'numeric', 'octet_length',
            'of', 'offset', 'old', 'on', 'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay',
            'parameter', 'partition', 'percentile_cont', 'percentile_disc', 'percent_rank', 'position', 'power',
            'precision', 'prepare', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive', 'ref',
            'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2',
            'regr_slope', 'regr_sxx', 'regr_sxy', 'release', 'replace', 'result', 'return', 'returns', 'revoke',
            'right', 'rollback', 'rollup', 'row', 'rows', 'row_number', 'savepoint', 'schemas', 'scope', 'scroll',
            'search', 'second', 'select', 'sensitive', 'session_user', 'set', 'show', 'similar', 'smallint', 'some',
            'specific', 'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static',
            'stddev_pop', 'stddev_samp', 'submultiset', 'substring', 'sum', 'symmetric', 'system', 'system_user',
            'table', 'tables', 'tablesample', 'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute',
            'tinyint', 'to', 'trailing', 'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'uescape',
            'union', 'unique', 'unknown', 'unnest', 'update', 'upper', 'use', 'user', 'using', 'value', 'values',
            'varbinary', 'varchar', 'varying', 'var_pop', 'var_samp', 'when', 'whenever', 'where', 'width_bucket',
            'window', 'with', 'within', 'without', 'year'
        ]
    )

    def __init__(self, dialect):
        super(SolrIdentifierPreparer, self).__init__(dialect, initial_quote='`', final_quote='`')



class SolrDialect(default.DefaultDialect):
    name = 'solrdbapi'
    driver = 'rest'
    dbapi = ""
    preparer = SolrIdentifierPreparer
    statement_compiler = SolrCompiler
    poolclass = pool.SingletonThreadPool
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(self, **kw):
        default.DefaultDialect.__init__(self, **kw)
        self.supported_extensions = []

    @classmethod
    def dbapi(cls):
        import sqlalchemy_solr.solrdbapi as module
        return module

    def create_connect_args(self, url, **kwargs):
        url_port = url.port or 8047
        qargs = {'host': url.host, 'port': url_port}

        try:
            db_parts = (url.database or 'solr').split('/')
            db = ".".join(db_parts)

            # Save this for later use.
            self.host = url.host
            self.port = url_port
            self.username = url.username
            self.password = url.password
            self.db = db

            # Get Storage Plugin Info:
            if db_parts[0]:
                self.storage_plugin = db_parts[0]

            if len(db_parts) > 1:
                self.workspace = db_parts[1]

            qargs.update(url.query)
            qargs['db'] = db
            if url.username:
                qargs['solruser'] = url.username
                qargs['solrpass'] = ""
                if url.password:
                    qargs['solrpass'] = url.password
        except Exception as ex:
            logging.error("Error in SolrDialect_http.create_connect_args :: " + str(ex))

        return [], qargs

    def do_rollback(self, dbapi_connection):
        # No transactions for Solr
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Solr has no support for foreign keys.  Returns an empty list."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Solr has no support for indexes.  Returns an empty list. """
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Solr has no support for primary keys.  Retunrs an empty list."""
        return []

    def get_schema_names(self, connection, **kw):
        return tuple(['default'])
        
    def get_table_names(self, connection, schema=None, **kw):
        session = Session()
        
        local_payload = api_globals._PAYLOAD.copy()
        local_payload['action'] = 'LIST'
        result = session.get(
            connection.raw_connection().proto + connection.raw_connection().host + ":" + str(connection.raw_connection().port) + "/" + 
                connection.raw_connection().server_path + "/admin/collections",
            params=local_payload,
            headers=api_globals._HEADER
        )
        try:
            tables_names = result.json()['collections']
            print(tables_names)
        except Exception as ex:
            logging.error("Error in SolrDialect_http.get_table_names :: " + str(ex))
        print(result)
        return tuple(tables_names)

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None):
        try:
            self.get_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            logging.error("Error in SolrDialect_http.has_table :: " + exc.NoSuchTableError)
            return False

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

    def object_as_dict(obj):
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def get_data_type(self, data_type):
        try:
            dt = _type_map[data_type]
        except:
            dt = types.UserDefinedType
        return dt

    def get_columns(self, connection, table_name, schema=None, **kw):
        result = []

        plugin_type = self.get_plugin_type(connection, schema)

        if plugin_type == "file":
            file_name = schema + "." + table_name
            quoted_file_name = self.identifier_preparer.format_solr_table(file_name, isFile=True)
            q = "SELECT * FROM {file_name} LIMIT 1".format(file_name=quoted_file_name)
            column_metadata = connection.execute(q).cursor.description

            for row in column_metadata:

                #  Get rid of precision information in data types
                data_type = row[1].lower()
                pattern = r"[a-zA-Z]+\(\d+, \d+\)"

                if re.search(pattern, data_type):
                    data_type = data_type.split('(')[0]
                solr_data_type = self.get_data_type(data_type)
                column = {
                    "name": row[0],
                    "type": solr_data_type,
                    "longtype": solr_data_type
                }
                result.append(column)
            return result

        elif "SELECT " in table_name:
            q = "SELECT * FROM ({table_name}) LIMIT 1".format(table_name=table_name)
        else:
            quoted_schema = self.identifier_preparer.format_solr_table(schema + "." + table_name, isFile=False)
            q = "DESCRIBE {table_name}".format(table_name=quoted_schema)

        query_results = connection.execute(q)

        for row in query_results:
            column = {
                "name": row[0],
                "type": self.get_data_type(row[1].lower()),
                "longType": self.get_data_type(row[1].lower())
            }
            result.append(column)
        return result

    def get_plugin_type(self, connection, plugin=None):
        if plugin is None:
            return

        try:
            query = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE SCHEMA_NAME LIKE '%" + plugin.replace(
                '`', '') + "%'"

            rows = connection.execute(query).fetchall()
            plugin_type = ""
            for row in rows:
                plugin_type = row[1]
                plugin_name = row[0]

            return plugin_type

        except Exception as ex:
            logging.error("Error in SolrDialect_http.get_plugin_type :: " + str(ex))
            return False

