from requests import Session
from sqlalchemy_solr import defaults


class SolrSpec:

    _spec = None

    def __init__(self, url):
        """
        Initializes a SolrSpec object

        :param url: Solr base url which can be a string HTTP(S) URL or a sqlalchemy.engine.url.URL.
        """

        session = Session()

        if isinstance(url, str):
            base_url = url
        else:
            if "verify_ssl" in url.query and url.query["verify_ssl"] in [
                "False",
                "false",
            ]:
                session.verify = False

            token = None
            if "token" in url.query:
                token = url.query["token"]

            if token is not None:
                session.headers.update({"Authorization": f"Bearer {token}"})
            else:
                session.auth = (url.username, url.password)

            proto = "http"
            if "use_ssl" in url.query and url.query["use_ssl"] in ["True", "true"]:
                proto = "https"

            server_path = url.database.split("/")[0]

            port = url.port or defaults.PORT
            base_url = f"{proto}://{url.host}:{port}/{server_path}"

        sys_info_response = session.get(
            base_url + "/admin/info/system", params={"wt": "json"}
        )

        spec_version = sys_info_response.json()["lucene"]["solr-spec-version"]
        self._spec = list(map(int, spec_version.split(".")))

    def __str__(self) -> str:
        return ".".join(list(map(str, self._spec)))

    def spec(self):
        return self._spec
