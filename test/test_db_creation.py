from requests import Session

from .steps import TestSteps


class TestDBCreation:
    def test_db_creation(self, settings):
        test_steps = TestSteps(settings)
        session = Session()
        headers = test_steps.login(session)
        db_creation_response = test_steps.create_database(session, headers)
        assert db_creation_response.status_code == 201
