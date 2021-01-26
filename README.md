# Apache Solr dialect for SQLAlchemy and Apache Superset

A dialect for ßApache Solr that can be used with [Apache Superset](https://superset.apache.org).

## Installation

The package can either be installed through PyPi or from the source code.

#### Through Python Package Index

`pip install sqlalchemy-solr`

#### Latest from Source Code

`pip install git+https://github.com/aadel/sqlalchemy-solr`

## Usage

To connect to Solr with SQLAlchemy, the following URL pattern can be used:

```
solr://<username>:<password>@<host>:<port>/solr/<collection>[?use_ssl=true|false]
```

## Testing

#### Requirements

* A Solr instance with a Parallel SQL supported up and running
* A Superset instance up and running with this package installed
* `pytest` >= 6.2.1 installed on the testing machine

#### Procedure

1. Change `conftest.py` as appropriate
2. Run `pytest`

## Resources
1. [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/index.html)
2. [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)
