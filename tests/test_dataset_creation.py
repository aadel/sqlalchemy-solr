from tests.setup import create_database

from .steps import TestSteps


class TestDatasetCreation:
    # pylint: disable=too-few-public-methods
    def test_dataset_creation(self, settings):
        test_steps = TestSteps(settings)
        session, headers, db_response = create_database(settings)
        database_id = db_response[0]["id"]
        dataset_creation_response = test_steps.create_dataset(
            session, headers, database_id
        )
        assert dataset_creation_response.status_code == 201
