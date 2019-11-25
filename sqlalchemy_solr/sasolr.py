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


# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import pool
from sqlalchemy.engine import default
from .base import SolrDialect, SolrIdentifierPreparer, SolrCompiler

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger

class SolrDialect_sasolr(SolrDialect):

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
        url_port = url.port or 8983
        qargs = {'host': url.host, 'port': url_port}

        try:
            db_parts = url.database.split('/')
            db = ".".join(db_parts)

            # Server path mapping
            if db_parts[0]:
                server_path = db_parts[0]

            # Mapping database to collection
            if db_parts[1]:
                collection = db_parts[1]

            # Save this for later use.
            self.host = url.host
            self.port = url_port
            self.username = url.username
            self.password = url.password
            self.db = db
            self.collection = collection
            self.server_path = server_path

            qargs.update(url.query)
            qargs['db'] = url.database
            qargs['collection'] = collection
            qargs['server_path'] = server_path
        except Exception as ex:
            print("************************************")
            print("Error in SolrDialect_sasolr.create_connect_args :: ", str(ex))
            print("************************************")
            raise ex
        return [], qargs
