# -*- coding: utf-8 -*-
from json import dumps
from numpy import nan
from pandas import DataFrame
from requests import Session
from pandas import to_datetime
import re
import logging

from . import api_globals
from .api_exceptions import Error, Warning, AuthError, DatabaseError, \
    ProgrammingError, CursorClosedException, ConnectionClosedException

from sqlalchemy_solr.message_formatter import MessageFormatter

apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'
default_storage_plugin = ""

# Python DB API 2.0 classes
class Cursor(object):

    mf = MessageFormatter()

    def __init__(self, host, db, username, password, server_path, collection, 
            port, proto, session, conn):

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
        self._resultSet = None
        self._resultSetMetadata = None
        self._resultSetStatus = None
        self.rowcount = -1
        self.lastrowid = None
        self.default_storage_plugin = None


    # Decorator for methods which require connection
    def connected(func): # pylint: disable=no-self-argument
        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                logging.error(self.mf.format("Error in Cursor.func_wrapper"))
                raise CursorClosedException("Cursor object is closed")
            elif self.connection._connected is False:
                logging.error(self.mf.format("Error in Cursor.func_wrapper"))
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs) # pylint: disable=not-callable

        return func_wrapper

    @staticmethod
    def substitute_in_query(string_query, parameters):
        query = string_query
        try:
            for param in parameters:
                if type(param) == str:
                    query = query.replace("?", "'{param}'".format(param=param), 1)
                else:
                    query = query.replace("?", str(param), 1)
        except Exception as ex:
            logging.error(Cursor.mf.format("Error in Cursor.substitute_in_query", str(ex)))
        return query

    @staticmethod
    def submit_query(query, host, port, proto, username, password, server_path, collection, session):
        local_payload = api_globals._PAYLOAD.copy()
        local_payload["stmt"] = query
        return session.get(
            proto + host + ":" + str(port) + "/" + server_path + "/" + collection + "/sql",
            params=local_payload,
            headers=api_globals._HEADER,
            auth=(username, password)
        )

    @staticmethod
    def parse_column_types(df):
        names = []
        types = []
        try:
            for column in df:
                names.append(column)
                try:
                    df[column] = df[column].astype(int)
                    types.append("bigint")
                except ValueError:
                    try:
                        df[column] = df[column].astype(float)
                        types.append("decimal")
                    except ValueError:
                        try:
                            df[column] = to_datetime(df[column])
                            types.append("timestamp")
                        except ValueError:
                            types.append("varchar")
        except Exception as ex:
            logging.error(Cursor.mf.format("Error in Cursor.parse_column_types", str(ex)))
        return names, types

    @connected
    def getdesc(self):
        return self.description

    @connected
    def close(self):
        self._connected = False

    @connected
    def execute(self, operation, parameters=()):
        result = self.submit_query(
            self.substitute_in_query(operation, parameters),
            self.host,
            self.port,
            self.proto,
            self.username,
            self.password,
            self.server_path,
            self.collection,
            self._session
        )

        logging.info(self.mf.format("Query:", operation))

        if result.status_code != 200:
            logging.error(self.mf.format("Error in Cursor.execute"))
            raise ProgrammingError(result.json().get("errorMessage", "ERROR"), result.status_code)
        else:
            rows = result.json()["result-set"]['docs']
            columns = []
            if "EOF" in rows[-1]:
                del rows[-1]
            if len(rows) > 0:
                columns=rows[0].keys()
            
            self._resultSet = (
                DataFrame(data=rows, columns=columns).fillna(value=nan)
            )

            column_names, column_types = self.parse_column_types(self._resultSet)

            # Get column metadata
            column_metadata = list(map(
                lambda cname, ctype: {"column": cname, "type": ctype}, column_names, column_types))

            self._resultSetMetadata = column_metadata
            self.rowcount = len(self._resultSet)
            self._resultSetStatus = iter(range(len(self._resultSet)))
            try:
                self.description = tuple(
                    zip(
                        column_names,
                        column_types,
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [True for i in range(len(self._resultSet.dtypes.index))]
                    )
                )
                return self
            except Exception as ex:
                logging.error(self.mf.format("Error in Cursor.execute", str(ex)))

    @connected
    def fetchone(self):
        try:
            # Added Tuple
            return self._resultSet.iloc[next(self._resultSetStatus)]
        except StopIteration:
            logging.error(self.mf.format("Catched StopIteration in fetchone"))
            # We need to put None rather than Series([]) because
            # SQLAlchemy processes that a row with no columns which it doesn't like
            return None

    @connected
    def fetchmany(self, size=None):

        if size is None:
            fetch_size = self.arraysize
        else:
            fetch_size = size

        try:
            index = next(self._resultSetStatus)
            try:
                for _ in range(fetch_size - 1):
                    next(self._resultSetStatus)
            except StopIteration:
                pass

            myresults = self._resultSet[index: index + fetch_size]
            return [tuple(x) for x in myresults.to_records(index=False)]
        except StopIteration:
            logging.error(self.mf.format("Catched StopIteration in fetchmany"))
            return None

    @connected
    def fetchall(self):
        # We can't just return a dataframe to sqlalchemy, it has to be a list of tuples...
        try:
            remaining = self._resultSet[next(self._resultSetStatus):]
            self._resultSetStatus = iter(tuple())
            return [tuple(x) for x in remaining.to_records(index=False)]

        except StopIteration:
            logging.error(self.mf.format("Catched StopIteration in fetchall"))
            return None

    @connected
    def get_query_metadata(self):
        return self._resultSetMetadata

    def get_default_plugin(self):
        return self.default_storage_plugin

    def __iter__(self):
        return self._resultSet.iterrows()


class Connection(object):
    
    mf = MessageFormatter()
    
    def __init__(self, host, db, username, password, server_path, 
            collection, port, proto, session):

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

    # Decorator for methods which require connection
    def connected(func): # pylint: disable=no-self-argument

        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                logging.error(self.mf.format("ConnectionClosedException in func_wrapper"))
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs) # pylint: disable=not-callable
        return func_wrapper

    def is_connected(self):
        try:
            if self._connected is True:
                if self._session:
                    return True
                else:
                    self._connected = False
        except Exception:
            logging.error(self.mf.format("Error in Connection.is_connected"))
            print(Exception)
        return False

    @connected
    def close_connection(self):
        try:
            self._session.close()
            self.close()
        except Exception as ex:
            logging.error(self.mf.format("Error in Connection.close_connection" + str(ex)))
            return False
        return True

    @connected
    def close(self):
        self._connected = False

    @connected
    def commit(self):
        if self._connected is False:
            print("AlreadyClosedException")
        else:
            print("Here goes some sort of commit")

    @connected
    def cursor(self):
        return Cursor(self.host, self.db, self.username, self.password, 
            self.server_path, self.collection, self.port, self.proto,
            self._session, self)


def connect(host, port=8047, db=None, username=None, password=None,
        server_path='solr', collection=None, use_ssl=False, 
        verify_ssl=False, ca_certs=None):

    session = Session()
    mf = MessageFormatter()

    if verify_ssl is False:
        session.verify = False
    else:
        if ca_certs is not None:
            session.verify = ca_certs
        else:
            session.verify = True

    if use_ssl in [True, 'True', 'true']:
        proto = "https://"
    else:
        proto = "http://"

    if collection is not None:
        local_url = "/" + server_path + "/" + collection + "/select"

        response = session.get(
            "{proto}{host}:{port}{url}".format(proto=proto, host=host, port=str(port), url=local_url),
            headers=api_globals._HEADER,
            auth=(username, password)
        )

        if response.status_code != 200:
            logging.error(mf.format("Error in connect"))
            logging.error(mf.format("Response code:", response.status_code))
            raise DatabaseError(str(response.json()["errorMessage"]), response.status_code)

    return Connection(host, db, username, password, server_path, 
            collection, port, proto, session)
