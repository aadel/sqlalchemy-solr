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

from requests import RequestException
from requests import Session
from sqlalchemy_solr.solrdbapi.api_exceptions import DatabaseError

from .api_globals import _HEADER
from .api_globals import _PAYLOAD
from .base import SolrDialect
from .message_formatter import MessageFormatter

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.ERROR)


class SolrDialect_http(SolrDialect):  # pylint: disable=invalid-name
    # pylint: disable=abstract-method,too-many-instance-attributes

    supports_statement_cache = True

    mf = MessageFormatter()

    def __init__(self, **kw):
        super().__init__(**kw)

        self.proto = None
        self.host = None
        self.port = None
        self.server_path = None
        self.collection = None
        self.db = None
        self.username = None
        self.password = None
        self.token = None
        self.session = None
        self.supported_extensions = []

        self.aliases = []

    def create_connect_args(self, url):

        url_port = url.port or 8047
        qargs = {"host": url.host, "port": url_port}

        db_parts = url.database.split("/")
        db = ".".join(db_parts)

        self.proto = "http://"
        if "use_ssl" in url.query:
            if url.query["use_ssl"] in [True, "True", "true"]:
                self.proto = "https://"

        if "token" in url.query:
            if url.query["token"] is not None:
                self.token = url.query["token"]

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

        # Prepare a session with proper authorization handling.
        session = Session()
        # session.verify property which is bydefault true so Handled here
        if "verify_ssl" in url.query and url.query["verify_ssl"] in [
            False,
            "False",
            "false",
        ]:
            session.verify = False

        if self.token is not None:
            session.headers.update({"Authorization": f"Bearer {self.token}"})
        else:
            session.auth = (self.username, self.password)
        # Utilize this session in other methods.
        self.session = session

        qargs.update(url.query)
        qargs["db"] = db
        qargs["server_path"] = server_path
        qargs["collection"] = collection
        qargs["username"] = url.username
        qargs["password"] = url.password

        return [], qargs

    def get_table_names(self, connection, schema=None, **kw):
        local_payload = _PAYLOAD.copy()
        self._get_aliases(local_payload)
        aliases_tuple = tuple(a for a in self.aliases)
        return self._get_collections(local_payload) + aliases_tuple

    def get_columns(self, connection, table_name, schema=None, **kw):
        local_payload = _PAYLOAD.copy()

        if "columns" in kw:
            columns = kw["columns"]
        else:
            columns = []
            self._get_aliases(local_payload)

        if table_name in self.aliases:
            for collection in self.aliases[table_name].split(","):
                self.get_columns(None, collection, columns=columns)

            return self.get_unique_columns(columns)

        local_payload["action"] = "LIST"
        try:
            result = self._session_get(local_payload, f"/{table_name}/admin/luke")

            fields = result.json()["fields"]
            for field in fields:
                column = {
                    "name": field,
                    "type": self.get_data_type(fields[field]["type"]),
                    "longType": self.get_data_type(fields[field]["type"]),
                }
                columns.append(column)

            return self.get_unique_columns(columns)
        except RequestException as e:
            raise DatabaseError(e.response.text) from e
        except (KeyError, TypeError) as e:
            raise e

    def _get_collections(self, local_payload):
        local_payload["action"] = "LIST"
        try:
            result = self._session_get(local_payload, "/admin/collections")
            collections_names = result.json()["collections"]
            return tuple(collections_names)
        except (RequestException, KeyError, TypeError) as e:
            logging.exception(e)
            return tuple()

    def _get_aliases(self, local_payload):
        local_payload["action"] = "LISTALIASES"
        try:
            result = self._session_get(local_payload, "/admin/collections")
            if "aliases" in result.json() and result.json()["aliases"]:
                self.aliases = result.json()["aliases"]
            # No aliases found or aliases are not supported
            else:
                self.aliases = ()
        except (RequestException, KeyError, TypeError) as e:
            logging.exception(e)

    def _session_get(self, payload, path: str):
        payload["wt"] = "json"
        try:
            response = self.session.get(
                self.proto
                + self.host
                + ":"
                + str(self.port)
                + "/"
                + self.server_path
                + path,
                params=payload,
                headers=_HEADER,
            )
            return response

        except RequestException as e:
            raise DatabaseError(e.response.text) from e

    def get_unique_columns(self, columns):
        unique_columns = []
        columns_set = {column["name"] for column in columns}
        for c in columns:
            if c["name"] in columns_set:
                unique_columns.append(c)
                columns_set.remove(c["name"])

        return unique_columns
