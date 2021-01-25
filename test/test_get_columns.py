import os
import json
from requests import Session
from .settings import (SUPERSET_USER, SUPERSET_PASS, 
    SUPERSET_URI, SUPERSET_DATABASE_NAME, SOLR_WORKER_COLLECTION_NAME)
from .steps import login, create_database, get_database, create_dataset

class TestGetColumns:

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
        

    def test_get_columns(self):
        session = Session()
        headers = login(session)
        create_database(session, headers)
        db_response = get_database(session, headers, SUPERSET_DATABASE_NAME)
        create_dataset(session, headers, db_response[0]['id'])
        datasets_response = session.get(SUPERSET_URI + '/api/v1/dataset',
            headers=headers)
        datasets = datasets_response.json()['result']
        sales_dataset = filter(lambda dataset: dataset['table_name'] == SUPERSET_DATABASE_NAME, datasets)

        dataset_response = session.get(SUPERSET_URI + '/api/v1/dataset/' \
            + str(list(sales_dataset)[0]['id']), headers=headers)
        dataset_colummns = list(map(lambda column: (column['column_name'], \
            column['type']), dataset_response.json()['result']['columns']))

        for column in self.COLUMNS:
            assert column in dataset_colummns
