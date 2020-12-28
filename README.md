# Apache Solr dialect for SQLAlchemy Superset

A dialect for Apache Solr that can be used with [Apache Superset](https://superset.incubator.apache.org).

## Installation

The driver can be either installed from the source code or through PyPi

### From the Source Code

1.  Clone or download this repository
2.  Navigate to the directory where you cloned the repository
3.  Run the python `setup.py` to install
4.  Restart Superset

```
git clone https://github.com/aadel/sqlalchemy-solr
cd sqlalchemy-solr
python3 setup.py install 

```

### Through Python Package Index

Run `pip install sqlalchemy-solr`

## Usage
To connect to Solr with SQLAlchemy, the following URL pattern can be used:

```
solr://<username>:<password>@<host>:<port>/solr/<collection>[?use_ssl=true|false]
```

## Resources
1. [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/index.html)
2. [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)
