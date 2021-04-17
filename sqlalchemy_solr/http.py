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
# -*- coding: utf-8 -*-
import logging

from requests import Session
from sqlalchemy.engine import default
from sqlalchemy_solr.api_globals import _HEADER
from sqlalchemy_solr.api_globals import _PAYLOAD

from .base import SolrDialect
from .message_formatter import MessageFormatter

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.ERROR)


class SolrDialect_http(SolrDialect):

    mf = MessageFormatter()

    def __init__(self, **kw):
        self.proto = None
        self.host = None
        self.port = None
        self.server_path = None
        self.collection = None
        self.db = None
        self.username = None
        self.password = None
        default.DefaultDialect.__init__(self, **kw)
        self.supported_extensions = []

    def create_connect_args(self, url, **kwargs):

        url_port = url.port or 8047
        qargs = {"host": url.host, "port": url_port}

        try:
            db_parts = url.database.split("/")
            db = ".".join(db_parts)

            self.proto = "http://"

            if "use_ssl" in kwargs:
                if kwargs["use_ssl"] in [True, "True", "true"]:
                    self.proto = "https://"

            # Mapping server path and collection
            if db_parts[0]:
                server_path = db_parts[0]
            else:
                raise AttributeError("Missing server path")
            if db_parts[1]:
                collection = db_parts[1]
            else:
                raise AttributeError("Missing collection")

            # Save this for later use.
            self.host = url.host
            self.port = url_port
            self.username = url.username
            self.password = url.password
            self.db = db
            self.server_path = server_path
            self.collection = collection

            qargs.update(url.query)
            qargs["db"] = db
            qargs["server_path"] = server_path
            qargs["collection"] = collection
            qargs["username"] = url.username
            qargs["password"] = url.password

        except Exception:
            logging.exception(
                self.mf.format("Error in SolrDialect_http.create_connect_args")
            )
        return [], qargs

    def get_table_names(self, connection, schema=None, **kw):
        session = Session()

        local_payload = _PAYLOAD.copy()
        local_payload["action"] = "LIST"
        try:
            result = session.get(
                self.proto
                + self.host
                + ":"
                + str(self.port)
                + "/"
                + self.server_path
                + "/admin/collections",
                params=local_payload,
                headers=_HEADER,
                auth=(self.username, self.password),
            )
            tables_names = result.json()["collections"]

            return tuple(tables_names)

        except Exception:
            logging.exception("Error in SolrDialect_http.get_table_names")

    def get_columns(self, connection, table_name, schema=None, **kw):
        columns = []

        session = Session()

        local_payload = _PAYLOAD.copy()
        local_payload["action"] = "LIST"
        try:
            result = session.get(
                self.proto
                + self.host
                + ":"
                + str(self.port)
                + "/"
                + self.server_path
                + "/"
                + table_name
                + "/admin/luke",
                params=local_payload,
                headers=_HEADER,
                auth=(self.username, self.password),
            )
            fields = result.json()["fields"]
            for field in fields:
                column = {
                    "name": field,
                    "type": self.get_data_type(fields[field]["type"]),
                    "longType": self.get_data_type(fields[field]["type"]),
                }
                columns.append(column)
            return columns
        except Exception:
            logging.exception("Error in SolrDialect_http.get_table_names")
