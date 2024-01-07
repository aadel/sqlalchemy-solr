# Apache Solr dialect for SQLAlchemy and Apache Superset

A dialect for Apache Solr that can be used with [Apache Superset](https://superset.apache.org).

## Installation

The package can either be installed through PyPi or from the source code.

#### Through Python Package Index

`pip install sqlalchemy-solr`

#### Latest from Source Code

`pip install git+https://github.com/aadel/sqlalchemy-solr`

## Usage

To connect to Solr with SQLAlchemy, the following URL pattern can be used:

```
solr://<username>:<password>@<host>:<port>/solr/<collection>[?parameter=value]
```

### Authentication

#### Basic Authentication

Basic authentication credentials can be supplied in connection URL as shown in the URL pattern above

#### JWT Authentication

JWT authentication token can be supplied as a `token` URL parameter, for example:

```
solr://<host>:<port>/solr/<collection>?token=<token_value>
```

### Additional Parameters:

If HTTPS is enable, the following parameters can be supplied:

1. `use_ssl`: a boolean parameter when set to `true` an HTTPS connection is used. Default value is `false`.
2. `verify_ssl`: a boolean parameter that controls whether to verify SSL certificate. Default value is `true`.

## Basic Example

The following example illustrates the basic usage in a Python project:

```
engine = create_engine('solr://solr:8983/solr/examples_books')

with engine.connect() as connection:
    result = connection.execute(text("SELECT sequence_i, genre_s FROM examples_books"))
    for row in result:
        print("Sequence: {}, Genre: {}".format(row['sequence_i'], row['genre_s']))
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
