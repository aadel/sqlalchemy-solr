from .steps import TestSteps


class TestDBCreation:
    # pylint: disable=too-few-public-methods
    def test_db_creation(self, settings, http):
        test_steps = TestSteps(settings)
        db_creation_response = test_steps.create_database(
            http["session"], http["headers"]
        )
        assert db_creation_response.status_code == 201
