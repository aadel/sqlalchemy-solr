from requests import Session
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from tests.fixtures.fixtures import SalesFixture
from tests.steps import TestSteps


def _index_data(settings):
    f = SalesFixture(settings)
    f.truncate_collection()
    f.index()


def prepare_orm(settings):
    _index_data(settings)
    metadata = MetaData()
    engine = create_engine(settings["SOLR_CONNECTION_URI"])
    t = Table(
        settings["SOLR_WORKER_COLLECTION_NAME"],
        metadata,
        autoload_with=engine,
    )

    return engine, t


def create_database(settings):
    test_steps = TestSteps(settings)
    session = Session()
    headers = test_steps.login(session)
    test_steps.create_database(session, headers)
    db_response = test_steps.get_database(
        session, headers, settings["SUPERSET_DATABASE_NAME"]
    )

    return session, headers, db_response
