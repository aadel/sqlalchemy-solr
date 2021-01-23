import os
import json
from requests import Session


class TestDBCreation:
    SOLR_WORKER_COLLECTION_NAME = "sales_test_"
    SUPERSET_URI = 'http://superset:8088'
    SUPERSET_USER = os.environ['SUPERSET_USER']
    SUPERSET_PASS = os.environ['SUPERSET_PASS']
    SUPERSET_DATABASE_NAME = 'sales_test_'

    COLUMNS = [('ADDRESSLINE1_s', 'VARCHAR'), ('ADDRESSLINE2_s', 'VARCHAR'), ('CITY_s', 'VARCHAR'),
        ('CONTACTFIRSTNAME_s', 'VARCHAR'), ('CONTACTLASTNAME_s', 'VARCHAR'), ('COUNTRY_CODE_s', 'VARCHAR'),
        ('COUNTRY_s', 'VARCHAR'), ('CUSTOMERNAME_s', 'VARCHAR'), ('DEALSIZE_s', 'VARCHAR'),
        ('MONTH_ID_i', 'INTEGER'), ('MSRP_i', 'INTEGER'), ('ORDERDATE_dt', 'DATETIME'),
        ('ORDERDATE_s', 'VARCHAR'), ('ORDERLINENUMBER_i', 'INTEGER'), ('ORDERNUMBER_i', 'INTEGER'),
        ('PHONE_s', 'VARCHAR'), ('POSTALCODE_s', 'VARCHAR'), ('PRICEEACH_f', 'FLOAT'),
        ('PRODUCTCODE_s', 'VARCHAR'), ('PRODUCTLINE_s', 'VARCHAR'), ('QTR_ID_i', 'INTEGER'),
        ('QUANTITYORDERED_i', 'INTEGER'), ('SALES_f', 'FLOAT'), ('STATE_s', 'VARCHAR'),
        ('STATUS_s', 'VARCHAR'), ('TERRITORY_s', 'VARCHAR'), ('YEAR_ID_i', 'INTEGER'),
        ('_root_', 'VARCHAR'), ('_version_', 'BIGINT'), ('id', 'VARCHAR')]
        
    session = Session()

    def test_db_creation(self):

        login_data = {
            "username": self.SUPERSET_USER,
            "password": self.SUPERSET_PASS,
            "provider": "db", "refresh": True
        }
        login_response = self.session.post(self.SUPERSET_URI + '/api/v1/security/login', 
            json=login_data)
        headers = {"Authorization": "Bearer " + login_response.json()['access_token']}
        datasets_response = self.session.get(self.SUPERSET_URI + '/api/v1/dataset',
            headers=headers)
        datasets = datasets_response.json()['result']
        sales_dataset = filter(lambda dataset: dataset['table_name'] == self.SUPERSET_DATABASE_NAME, datasets)

        dataset_response = self.session.get(self.SUPERSET_URI + '/api/v1/dataset/' \
            + str(list(sales_dataset)[0]['id']), headers=headers)
        dataset_colummns = list(map(lambda column: (column['column_name'], \
            column['type']), dataset_response.json()['result']['columns']))

        for column in self.COLUMNS:
            assert column in dataset_colummns