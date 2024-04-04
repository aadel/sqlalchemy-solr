import logging

from numpy import nan
from pandas import DataFrame
from requests import Session

from ..admin.solr_spec import SolrSpec
from ..api_globals import _HEADER
from ..api_globals import _PAYLOAD
from ..message_formatter import MessageFormatter
from .api_exceptions import ConnectionClosedException
from .api_exceptions import CursorClosedException
from .api_exceptions import DatabaseHTTPError
from .api_exceptions import ProgrammingError
from .api_exceptions import UninitializedResultSetError
from .solr_reflect import SolrTableReflection

apilevel = "2.0"  # pylint: disable=invalid-name
threadsafety = 3  # pylint: disable=invalid-name
paramstyle = "qmark"  # pylint: disable=invalid-name
default_storage_plugin = ""  # pylint: disable=invalid-name


# Python DB API 2.0 classes
class Cursor:
    # pylint: disable=too-many-instance-attributes

    mf = MessageFormatter()

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        host,
        db,
        username,
        password,
        server_path,
        collection,
        port,
        proto,
        session,
        conn,
    ):

        self.arraysize = 1
        self.db = db
        self.username = username
        self.password = password
        self.server_path = server_path
        self.collection = collection
        self.description = None
        self.host = host
        self.port = port
        self.proto = proto
        self._session = session
        self._connected = True
        self.connection = conn
        self._result_set = None
        self._result_set_metadata = None
        self._result_set_status = None
        self.rowcount = -1
        self.lastrowid = None
        self.default_storage_plugin = None

    @property
    def connected(self):
        return self._connected

    # Decorator for methods which require connection
    def connected_(func):  # pylint: disable=no-self-argument # noqa: B902
        def func_wrapper(self, *args, **kwargs):
            if self.connected is False:
                raise CursorClosedException("Cursor object is closed")
            if self.connection.connected is False:
                raise ConnectionClosedException("Connection object is closed")

            return func(self, *args, **kwargs)  # pylint: disable=not-callable

        return func_wrapper

    @staticmethod
    def substitute_in_query(string_query, parameters):
        query = string_query
        for param in parameters:
            if isinstance(param, str):
                query = query.replace("?", f"{param!r}", 1)
            else:
                query = query.replace("?", str(param), 1)
        return query

    @staticmethod
    # pylint: disable=too-many-arguments
    def submit_query(query, host, port, proto, server_path, collection, session):
        local_payload = _PAYLOAD.copy()
        local_payload["stmt"] = query
        return session.get(
            proto
            + host
            + ":"
            + str(port)
            + "/"
            + server_path
            + "/"
            + collection
            + "/sql",
            params=local_payload,
            headers=_HEADER,
        )

    @connected_
    def getdesc(self):
        return self.description

    @connected_
    def close(self):
        self._connected = False

    @connected_
    def execute(self, operation, parameters=()):
        """
        Prepare and execute a database query.

        Parameters may be provided as sequence and will be
        bound to variables in the query. Variables are specified in a
        question mark notation.

        Args:
             operation (str): The query to be executed
             parameters (Tuple): The query parameters
        """
        result = self.submit_query(
            self.substitute_in_query(operation, parameters),
            self.host,
            self.port,
            self.proto,
            self.server_path,
            self.collection,
            self._session,
        )

        logging.info(self.mf.format("Query:", operation))

        if result.status_code != 200:
            raise DatabaseHTTPError(result.text, result.status_code)

        rows = result.json()["result-set"]["docs"]

        if "EXCEPTION" in rows[0]:
            raise ProgrammingError(rows[0]["EXCEPTION"])

        columns = []
        if "EOF" in rows[-1]:
            del rows[-1]
        if len(rows) > 0:
            columns = rows[0].keys()

        self._result_set = DataFrame(data=rows, columns=columns).fillna(value=nan)

        column_names, column_types = SolrTableReflection.reflect_column_types(
            self._result_set, operation
        )

        # Get column metadata
        column_metadata = list(
            map(
                lambda cname, ctype: {"column": cname, "type": ctype},
                column_names,
                column_types,
            )
        )

        self._result_set_metadata = column_metadata
        self.rowcount = len(self._result_set)
        self._result_set_status = iter(range(len(self._result_set)))
        self.description = tuple(
            zip(  # noqa: B905
                column_names,
                column_types,
                [None for _ in range(len(self._result_set.dtypes.index))],
                [None for _ in range(len(self._result_set.dtypes.index))],
                [None for _ in range(len(self._result_set.dtypes.index))],
                [None for _ in range(len(self._result_set.dtypes.index))],
                [True for _ in range(len(self._result_set.dtypes.index))],
            )
        )
        return self

    @connected_
    def fetchone(self):
        """Fetches the next row of a query result set, returning a single object,
        or None when no more data is available.

        An UninitializedResultSetError is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet."""

        if self._result_set is None:
            raise UninitializedResultSetError("Resultset not initialized")

        try:
            return self._result_set.iloc[next(self._result_set_status)]
        except StopIteration:
            return None

    @connected_
    def fetchmany(self, size=None):
        """Fetches the next set of rows of a query result, returning a list of tuples.
        An empty list is returned when no more rows are available.

        Args:
            size (int): The size of the result subset to return. Default is 1.

        Returns:
            list(tuple): The result subset.

        An UninitializedResultSetError exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        if self._result_set is None:
            raise UninitializedResultSetError("Resultset not initialized")

        if size is None:
            fetch_size = self.arraysize
        else:
            fetch_size = size

        try:
            index = next(self._result_set_status)
            try:
                for _ in range(fetch_size - 1):
                    next(self._result_set_status)
            except StopIteration:
                pass

            # pylint: disable=unsubscriptable-object
            result_subset = self._result_set[index : index + fetch_size]
            return [tuple(x) for x in result_subset.to_records(index=False)]
        except StopIteration:
            return []

    @connected_
    def fetchall(self):
        """
        Fetches all remaining rows of a query result.

        An UninitializedResultSetError exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        if self._result_set is None:
            raise UninitializedResultSetError("Resultset not initialized")

        try:
            # pylint: disable=unsubscriptable-object
            remaining = self._result_set[next(self._result_set_status) :]
            self._result_set_status = iter(tuple())
            return [tuple(x) for x in remaining.to_records(index=False)]

        except StopIteration:
            return []

    @connected_
    def get_query_metadata(self):
        return self._result_set_metadata

    def get_default_plugin(self):
        return self.default_storage_plugin

    def __iter__(self):
        return self._result_set.iterrows()


class Connection:
    # pylint: disable=too-many-instance-attributes

    solr_spec = None
    mf = MessageFormatter()

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        host,
        db,
        username,
        password,
        server_path,
        collection,
        port,
        proto,
        session,
    ):

        self.host = host
        self.db = db
        self.username = username
        self.password = password
        self.server_path = server_path
        self.collection = collection
        self.proto = proto
        self.port = port
        self._session = session
        self._connected = True

        Connection.solr_spec = SolrSpec(f"{proto}{host}:{port}/{server_path}")

        SolrTableReflection.connection = self

    @property
    def session(self):
        return self._session

    @property
    def connected(self):
        return self._connected

    # Decorator for methods which require connection
    def connected_(func):  # pylint: disable=no-self-argument # noqa: B902
        def func_wrapper(self, *args, **kwargs):
            if self.connected is False:
                logging.error(
                    self.mf.format("ConnectionClosedException in func_wrapper")
                )
                raise ConnectionClosedException("Connection object is closed")

            return func(self, *args, **kwargs)  # pylint: disable=not-callable

        return func_wrapper

    @connected_
    def close(self):
        self._connected = False

    @connected_
    def commit(self):
        """
        Solr does not support commit in the transactional context
        """

    @connected_
    def rollback(self):
        """
        Solr does not support rollback
        """

    @connected_
    def cursor(self):
        return Cursor(
            self.host,
            self.db,
            self.username,
            self.password,
            self.server_path,
            self.collection,
            self.port,
            self.proto,
            self._session,
            self,
        )


# pylint: disable=too-many-arguments
def connect(
    host,
    port=8047,
    db=None,
    username=None,
    password=None,
    server_path="solr",
    collection=None,
    use_ssl=False,
    verify_ssl=None,
    token=None,
):

    session = Session()
    # bydefault session.verify is set to True
    if verify_ssl is not None and verify_ssl in [False, "False", "false"]:
        session.verify = False

    if use_ssl in [True, "True", "true"]:
        proto = "https://"
    else:
        proto = "http://"

    if collection is not None:
        local_url = "/" + server_path + "/" + collection + "/select"

        add_authorization(session, username, password, token)
        response = session.get(
            f"{proto}{host}:{port}{local_url}",
            headers=_HEADER,
        )

        if response.status_code != 200:
            raise DatabaseHTTPError(response.text, response.status_code)

    return Connection(
        host, db, username, password, server_path, collection, port, proto, session
    )


def add_authorization(session, username, password, token):
    if token is not None:
        session.headers.update({"Authorization": f"Bearer {token}"})
    else:
        session.auth = (username, password)
