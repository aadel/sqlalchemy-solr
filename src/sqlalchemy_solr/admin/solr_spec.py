from requests import Session


class SolrSpec:

    _spec = None

    def __init__(self, solr_base_url):
        session = Session()
        sys_info_response = session.get(
            solr_base_url + "/admin/info/system", params={"wt": "json"}
        )
        spec_version = sys_info_response.json()["lucene"]["solr-spec-version"]
        self._spec = list(map(int, spec_version.split(".")))

    def __str__(self) -> str:
        return ".".join(list(map(str, self._spec)))

    def spec(self):
        return self._spec
