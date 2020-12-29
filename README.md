# Apache Solr dialect for SQLAlchemy Superset

A dialect for Apache Solr that can be used with [Apache Superset](https://superset.incubator.apache.org).

## Installation

The driver can either be installed through PyPi or from the source code.

### Through Python Package Index

`pip install sqlalchemy-solr`

### Latest from Source Code

`pip install git+https://github.com/aadel/sqlalchemy-solr`

## Usage

To connect to Solr with SQLAlchemy, the following URL pattern can be used:

```
solr://<username>:<password>@<host>:<port>/solr/<collection>[?use_ssl=true|false]
```

## Resources
1. [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/index.html)
2. [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)
