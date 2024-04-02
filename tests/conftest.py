import os

import pytest
from requests import Session
from tests.date_range_funcs import Parameters
from tests.setup import prepare_orm
from tests.steps import TestSteps


@pytest.fixture(scope="session")
def http(settings):  # pylint: disable=redefined-outer-name
    test_steps = TestSteps(settings)
    session = Session()
    headers = test_steps.login(session)

    return {"session": session, "headers": headers}


@pytest.fixture(scope="session")
def settings():
    solr_worker_collection_name = "sales_test_"
    if os.environ.get("SOLR_USER"):
        # pylint: disable=consider-using-f-string
        solr_connection_uri = (
            "solr://{user}:{passwd}@solr:8983/solr/{collection}".format(
                user=os.environ.get("SOLR_USER"),
                passwd=os.environ.get("SOLR_PASS"),
                collection=solr_worker_collection_name,
            )
        )
    else:
        solr_connection_uri = "solr://solr:8983/solr/" + solr_worker_collection_name

    return {
        "HOST": "solr",
        "PORT": 8983,
        "PROTO": "http://",
        "SOLR_USER": os.environ.get("SOLR_USER"),
        "SOLR_PASS": os.environ.get("SOLR_PASS"),
        "SERVER_PATH": "solr",
        "SOLR_BASE_URL": "http://solr:8983/solr",
        "SOLR_CONNECTION_URI": solr_connection_uri,
        "SOLR_WORKER_COLLECTION_NAME": solr_worker_collection_name,
        "SUPERSET_URI": "http://superset:8088",
        "SUPERSET_USER": os.environ["SUPERSET_USER"],
        "SUPERSET_PASS": os.environ["SUPERSET_PASS"],
        "SUPERSET_DATABASE_NAME": "sales_test_",
    }


@pytest.fixture(scope="class")
def parameters(settings):  # pylint: disable=redefined-outer-name
    engine, t = prepare_orm(settings)

    lower_bound = "2017-05-10 00:00:00"
    upper_bound = "2017-05-20 00:00:00"
    lower_bound_iso = "2017-05-10T00:00:00Z"
    upper_bound_iso = "2017-05-20T00:00:00Z"
    select_statement_1 = "SELECT sales_test_.`CITY_s` \nFROM sales_test_ \nWHERE TRUE "
    select_statement_2 = (
        "SELECT sales_test_.`CITY_s` \nFROM sales_test_ "
        "\nWHERE sales_test_.`ORDERDATE_dt` = "
    )

    p = Parameters(
        engine,
        t,
        lower_bound,
        upper_bound,
        lower_bound_iso,
        upper_bound_iso,
        [select_statement_1, select_statement_2],
    )

    return p
