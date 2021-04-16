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
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import logging

from dateutil import parser
from sqlalchemy import exc
from sqlalchemy import inspect
from sqlalchemy import pool
from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators
from sqlalchemy.sql.expression import BindParameter
from sqlalchemy_solr.solr_type_compiler import SolrTypeCompiler
from sqlalchemy_solr.solrdbapi.array import ARRAY

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.ERROR)

_type_map = {
    "binary": types.LargeBinary(),
    "boolean": types.Boolean(),
    "pdate": types.DateTime(),
    "pint": types.Integer(),
    "plong": types.BigInteger(),
    "pfloat": types.Float(),
    "pdouble": types.REAL(),
    "string": types.VARCHAR(),
    "text_general": types.Text(),
    "booleans": ARRAY(types.BOOLEAN()),
    "pints": ARRAY(types.Integer()),
    "plongs": ARRAY(types.BigInteger()),
    "pfloats": ARRAY(types.Float()),
    "pdoubles": ARRAY(types.REAL()),
    "strings": ARRAY(types.VARCHAR()),
}


class SolrCompiler(compiler.SQLCompiler):

    merge_ops = (operators.ge, operators.gt, operators.le, operators.lt)
    bounds = {
        operators.ge: "[",
        operators.gt: "{",
        operators.le: "]",
        operators.lt: "}",
    }

    def default_from(self):
        """Called when a ``SELECT`` statement has no froms,
        and no ``FROM`` clause is to be appended.
        """
        return " FROM (values(1))"

    def visit_binary(self, binary, override_operator=None, eager_grouping=False, **kw):
        if binary.operator not in self.merge_ops:
            return super().visit_binary(binary, override_operator, eager_grouping, **kw)

        if str(binary.left) in kw:
            if kw[str(binary.left)].keys() & [operators.ge, operators.gt]:
                if kw[str(binary.left)].keys() & [operators.le, operators.lt]:
                    if binary.operator in (operators.ge, operators.gt):
                        # Boundary to be merged
                        return "TRUE"

        try:
            if isinstance(binary.right, BindParameter):
                datetime = parser.parse(binary.right.effective_value)
            else:
                datetime = parser.parse(binary.right.text)
        except (ValueError, TypeError):
            return super().visit_binary(binary, override_operator, eager_grouping, **kw)
        else:
            if binary.operator in (operators.ge, operators.gt):
                ldatetime = datetime
                lbound = self.bounds[binary.operator]
                if str(binary.left) in kw:
                    if kw[str(binary.left)].keys() & [operators.le, operators.lt]:
                        ubound, uoperator = (
                            ("]", operators.le)
                            if operators.le in kw[str(binary.left)]
                            else ("}", operators.lt)
                        )
                        if isinstance(
                            kw[str(binary.left)][uoperator].right.text, BindParameter
                        ):
                            udatetime = parser.parse(
                                kw[str(binary.left)][uoperator].right.effective_value
                            )
                        else:
                            udatetime = parser.parse(
                                kw[str(binary.left)][uoperator].right.text
                            )
                    else:
                        ubound, udatetime = "]", "*"
            else:
                udatetime = datetime
                ubound = self.bounds[binary.operator]
                if str(binary.left) in kw:
                    if kw[str(binary.left)].keys() & [operators.ge, operators.gt]:
                        lbound, loperator = (
                            ("[", operators.ge)
                            if operators.ge in kw[str(binary.left)]
                            else ("{", operators.gt)
                        )
                        if isinstance(
                            kw[str(binary.left)][loperator].right, BindParameter
                        ):
                            ldatetime = parser.parse(
                                kw[str(binary.left)][loperator].right.effective_value
                            )
                        else:
                            ldatetime = parser.parse(
                                kw[str(binary.left)][loperator].right.text
                            )
                    else:
                        lbound, ldatetime = "[", "*"

            binary.right = expression.TextClause(
                "'"
                + lbound
                + ldatetime.isoformat()
                + "Z TO "
                + udatetime.isoformat()
                + "Z"
                + ubound
                + "'"
            )
            binary.operator = operators.eq
            return super().visit_binary(binary, override_operator, eager_grouping, **kw)

    def visit_clauselist(self, clauselist, **kw):
        if clauselist.operator == operators.and_:
            for c in clauselist.clauses:
                if isinstance(c, expression.BinaryExpression):
                    kw[str(c.left)] = {} if str(c.left) not in kw else kw[str(c.left)]
                    try:
                        if isinstance(c.right, BindParameter):
                            parser.parse(c.right.effective_value)
                        else:
                            parser.parse(c.right.text)
                        kw[str(c.left)][c.operator] = c
                    except (AttributeError, ValueError, TypeError):
                        continue

        return super().visit_clauselist(clauselist, **kw)

    def visit_array(self, element, **kw):
        return "ARRAY[%s]" % self.visit_clauselist(element, **kw)


class SolrIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(
        [
            "abs",
            "all",
            "allocate",
            "allow",
            "alter",
            "and",
            "any",
            "are",
            "array",
            "as",
            "asensitive",
            "asymmetric",
            "at",
            "atomic",
            "authorization",
            "avg",
            "begin",
            "between",
            "bigint",
            "binary",
            "bit",
            "blob",
            "boolean",
            "both",
            "by",
            "call",
            "called",
            "cardinality",
            "cascaded",
            "case",
            "cast",
            "ceil",
            "ceiling",
            "char",
            "character",
            "character_length",
            "char_length",
            "check",
            "clob",
            "close",
            "coalesce",
            "collate",
            "collect",
            "column",
            "commit",
            "condition",
            "connect",
            "constraint",
            "convert",
            "corr",
            "corresponding",
            "count",
            "covar_pop",
            "covar_samp",
            "create",
            "cross",
            "cube",
            "cume_dist",
            "current",
            "current_catalog",
            "current_date",
            "current_default_transform_group",
            "current_path",
            "current_role",
            "current_schema",
            "current_time",
            "current_timestamp",
            "current_transform_group_for_type",
            "current_user",
            "cursor",
            "cycle",
            "databases",
            "date",
            "day",
            "deallocate",
            "dec",
            "decimal",
            "declare",
            "default",
            "default_kw",
            "delete",
            "dense_rank",
            "deref",
            "describe",
            "deterministic",
            "disallow",
            "disconnect",
            "distinct",
            "double",
            "drop",
            "dynamic",
            "each",
            "element",
            "else",
            "end",
            "end_exec",
            "escape",
            "every",
            "except",
            "exec",
            "execute",
            "exists",
            "exp",
            "explain",
            "external",
            "extract",
            "false",
            "fetch",
            "files",
            "filter",
            "first_value",
            "float",
            "floor",
            "for",
            "foreign",
            "free",
            "from",
            "full",
            "function",
            "fusion",
            "get",
            "global",
            "grant",
            "group",
            "grouping",
            "having",
            "hold",
            "hour",
            "identity",
            "if",
            "import",
            "in",
            "indicator",
            "inner",
            "inout",
            "insensitive",
            "insert",
            "int",
            "integer",
            "intersect",
            "intersection",
            "interval",
            "into",
            "is",
            "jar",
            "join",
            "language",
            "large",
            "last_value",
            "lateral",
            "leading",
            "left",
            "like",
            "limit",
            "ln",
            "local",
            "localtime",
            "localtimestamp",
            "lower",
            "match",
            "max",
            "member",
            "merge",
            "method",
            "min",
            "minute",
            "mod",
            "modifies",
            "module",
            "month",
            "multiset",
            "national",
            "natural",
            "nchar",
            "nclob",
            "new",
            "no",
            "none",
            "normalize",
            "not",
            "null",
            "nullif",
            "numeric",
            "octet_length",
            "of",
            "offset",
            "old",
            "on",
            "only",
            "open",
            "or",
            "order",
            "out",
            "outer",
            "over",
            "overlaps",
            "overlay",
            "parameter",
            "partition",
            "percentile_cont",
            "percentile_disc",
            "percent_rank",
            "position",
            "power",
            "precision",
            "prepare",
            "primary",
            "procedure",
            "range",
            "rank",
            "reads",
            "real",
            "recursive",
            "ref",
            "references",
            "referencing",
            "regr_avgx",
            "regr_avgy",
            "regr_count",
            "regr_intercept",
            "regr_r2",
            "regr_slope",
            "regr_sxx",
            "regr_sxy",
            "release",
            "replace",
            "result",
            "return",
            "returns",
            "revoke",
            "right",
            "rollback",
            "rollup",
            "row",
            "rows",
            "row_number",
            "savepoint",
            "schemas",
            "scope",
            "scroll",
            "search",
            "second",
            "select",
            "sensitive",
            "session_user",
            "set",
            "show",
            "similar",
            "smallint",
            "some",
            "specific",
            "specifictype",
            "sql",
            "sqlexception",
            "sqlstate",
            "sqlwarning",
            "sqrt",
            "start",
            "static",
            "stddev_pop",
            "stddev_samp",
            "submultiset",
            "substring",
            "sum",
            "symmetric",
            "system",
            "system_user",
            "table",
            "tables",
            "tablesample",
            "then",
            "time",
            "timestamp",
            "timezone_hour",
            "timezone_minute",
            "tinyint",
            "to",
            "trailing",
            "translate",
            "translation",
            "treat",
            "trigger",
            "trim",
            "true",
            "uescape",
            "union",
            "unique",
            "unknown",
            "unnest",
            "update",
            "upper",
            "use",
            "user",
            "using",
            "value",
            "values",
            "varbinary",
            "varchar",
            "varying",
            "var_pop",
            "var_samp",
            "when",
            "whenever",
            "where",
            "width_bucket",
            "window",
            "with",
            "within",
            "without",
            "year",
        ]
    )

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`", final_quote="`")


class SolrDialect(default.DefaultDialect):
    name = "solrdbapi"
    driver = "rest"
    preparer = SolrIdentifierPreparer
    statement_compiler = SolrCompiler
    type_compiler = SolrTypeCompiler
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
        return tuple(["default"])

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None):
        try:
            self.get_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            logging.exception("Error in SolrDialect_http.has_table")
            return False

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

    def object_as_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

    def get_data_type(self, data_type):
        try:
            dt = _type_map[data_type]
        except Exception:
            dt = types.UserDefinedType
        return dt
