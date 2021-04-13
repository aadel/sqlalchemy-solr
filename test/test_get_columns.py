from requests import Session

from .steps import TestSteps


class TestGetColumns:

    COLUMNS = [
        ("ADDRESSLINE1_s", "VARCHAR"),
        ("ADDRESSLINE2_s", "VARCHAR"),
        ("CITY_s", "VARCHAR"),
        ("CONTACTFIRSTNAME_s", "VARCHAR"),
        ("CONTACTLASTNAME_s", "VARCHAR"),
        ("COUNTRY_CODE_s", "VARCHAR"),
        ("COUNTRY_s", "VARCHAR"),
        ("CUSTOMERNAME_s", "VARCHAR"),
        ("DEALSIZE_s", "VARCHAR"),
        ("MONTH_ID_i", "INTEGER"),
        ("MSRP_i", "INTEGER"),
        ("ORDERDATE_dt", "DATETIME"),
        ("ORDERDATE_s", "VARCHAR"),
        ("ORDERLINENUMBER_i", "INTEGER"),
        ("ORDERNUMBER_i", "INTEGER"),
        ("PHONE_ss", "VARCHAR"),
        ("POSTALCODE_s", "VARCHAR"),
        ("PRICEEACH_f", "FLOAT"),
        ("PRODUCTCODE_s", "VARCHAR"),
        ("PRODUCTLINE_s", "VARCHAR"),
        ("QTR_ID_i", "INTEGER"),
        ("QUANTITYORDERED_i", "INTEGER"),
        ("SALES_f", "FLOAT"),
        ("STATE_s", "VARCHAR"),
        ("STATUS_s", "VARCHAR"),
        ("TERRITORY_s", "VARCHAR"),
        ("YEAR_ID_i", "INTEGER"),
        ("_root_", "VARCHAR"),
        ("_version_", "BIGINT"),
        ("id", "VARCHAR"),
    ]

    def test_get_columns(self, settings):
        test_steps = TestSteps(settings)
        session = Session()
        headers = test_steps.login(session)
        test_steps.create_database(session, headers)
        db_response = test_steps.get_database(
            session, headers, settings["SUPERSET_DATABASE_NAME"]
        )
        test_steps.create_dataset(session, headers, db_response[0]["id"])
        datasets_response = session.get(
            settings["SUPERSET_URI"] + "/api/v1/dataset", headers=headers
        )
        datasets = datasets_response.json()["result"]
        sales_dataset = filter(
            lambda dataset: dataset["table_name"] == settings["SUPERSET_DATABASE_NAME"],
            datasets,
        )

        dataset_response = session.get(
            settings["SUPERSET_URI"]
            + "/api/v1/dataset/"
            + str(list(sales_dataset)[0]["id"]),
            headers=headers,
        )
        dataset_colummns = list(
            map(
                lambda column: (column["column_name"], column["type"]),
                dataset_response.json()["result"]["columns"],
            )
        )

        for column in self.COLUMNS:
            assert column in dataset_colummns
