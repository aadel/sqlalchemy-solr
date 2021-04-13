from requests import Session

from .steps import TestSteps


class TestDatasetCreation:
    def test_db_creation(self, settings):
        test_steps = TestSteps(settings)
        session = Session()
        headers = test_steps.login(session)
        test_steps.create_database(session, headers)
        db_response = test_steps.get_database(
            session, headers, settings["SUPERSET_DATABASE_NAME"]
        )
        database_id = db_response[0]["id"]
        dataset_creation_response = test_steps.create_dataset(
            session, headers, database_id
        )
        assert dataset_creation_response.status_code == 201
