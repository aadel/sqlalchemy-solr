import pytest
from sqlalchemy_solr.admin.solr_spec import SolrSpec


def assert_solr_release(settings, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])
    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )
