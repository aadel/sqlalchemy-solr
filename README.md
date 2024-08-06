[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

# Apache Solr dialect for SQLAlchemy

A [SQLAlchemy](https://www.sqlalchemy.org/) dialect for Apache Solr.

## Requirements

The dialect is compatible with Solr version 6.0 and higher.

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

_Note_: port 8983 is used when `port` in the URL is omitted

### Authentication

#### Basic Authentication

Basic authentication credentials can be supplied in connection URL as shown in the URL pattern above

#### JWT Authentication

JWT authentication token can be supplied as a `token` URL parameter, for example:

```
solr://<host>:<port>/solr/<collection>?token=<token_value>
```

### Additional Parameters:

If HTTPS is enabled, the following parameters can be supplied:

1. `use_ssl`: a boolean parameter when set to `true` an HTTPS connection is used. Default value is `false`.
2. `verify_ssl`: a boolean parameter that controls whether to verify SSL certificate. Default value is `true`.

### Aliases

Aliases are supported as tables where columns are the union of all the underlying collections fields. For example, if an alias `collection_alias` has linked collection members, the following query is valid:

```
SELECT f1, f2, f3 FROM collection_alias
```

where `f1`, `f2`, and `f3` are defined in the linked collections.

### Time Filters

Time filtration predicates are transformed to Solr syntax when ORM mode is used and Solr datasource release is lower than 9. Time filters transformations are handled internally by Solr 9 and above without the driver intervention. Open- and close-ended date ranges are supported.

### Multi-valued Fields

Multi-value fields are mapped to SQL arrays of a specific scalar type. For example:
```
phones = Column('PHONE_ss', ARRAY(String))
```

### SQL Compilation Caching

The dialect supports caching by leveraging SQLAlchemy SQL compilation caching capabilities, which include query caching.

### Schema

If the ORM query supplied explicitly refers to a schema, the schema would be filtered out before query execution.

## Basic Example

The following example illustrates the basic usage in a Python project:

```
engine = create_engine('solr://solr:8983/solr/examples_books')

with engine.connect() as connection:
    result = connection.execute(text("SELECT sequence_i, genre_s FROM examples_books"))
    for row in result:
        print("Sequence: {}, Genre: {}".format(row['sequence_i'], row['genre_s']))
```

## ORM Example

```
Base = declarative_base()

class Book(Base):
    __tablename__ = "books"

    id = Column('index_i', Integer, primary_key=True)
    name = Column('book_name_t', String)
    publishing_year = Column('publishing_year_i', Integer)

    def __repr__(self):
        return f"Book(id={self.id!r}, name={self.name!r}, publishing_year={self.publishing_year!r})"

engine = create_engine('solr://solr:8983/solr/books')

with Session(engine) as session:
    stmt = select(Book).where(Book.publishing_year.in_([2014, 2015])).limit(10)

    for book in session.scalars(stmt):
        print(book)
```
where `index_i`, `book_name_t`, and `publishing_year_i` are fields of `books` collection.

## Time Filters Example

```
Base = declarative_base()

class SalesHistory(Base):
    __tablename__ = "sales"

    order_number = Column('order_number_i', Integer, primary_key=True)
    price_each = Column('price_each_f', Float)
    status = Column('status_s', String)
    order_date = Column('order_date_dt', Integer)

    def __repr__(self):
        return f"SalesHistory(order number={self.order_number!r}, status={self.status!r}, price={self.price_each}, order date={self.order_date!r})"

engine = create_engine('solr://solr:8983/solr/sales')

with Session(engine) as session:
    stmt = select(SalesHistory) \
        .where(and_(SalesHistory.order_date >= "2024-01-01 00:00:00", SalesHistory.order_date < "2024-02-01 00:00:00")) \
        .order_by(SalesHistory.price_each.asc()) \
        .limit(10)

    for row in session.scalars(stmt):
        print(row)
```
where `order_number_i`, `price_each_f`, `status_s`, and `order_date_dt` fields of `sales` collection.

In the above example, date predicates are transformed to `[2024-01-01T00:00:00Z TO 2024-02-01T00:00:00Z}`.

Open-ended date ranges are supported. For example:

```
.where(SalesHistory.order_date >= "2024-01-01 00:00:00")
```

translates to `[2024-01-01T00:00:00Z TO *]`

## Operator Translation
`ILIKE` and `NOT ILIKE` operators are translated to `LIKE` amd `NOT LIKE` operators which are case in-sensetive in Solr.

## Miscellaneous
SQL statement semi-colon is removed if present.

## Compatibility

| Feature                          | 6.0 | 6.5 | 6.6 | 7.x | 8.x | 9.x |
|----------------------------------|-----|-----|-----|-----|-----|-----|
| Aliases                          |  ✗  |  ✗  |  ✓  |  ✓  |  ✓  |  ✓  |
| Built-in date range compilation  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✓  |
| `SELECT` _expression_ statements |  ✗  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| SQL compilation caching          |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✓  |

## Use Cases

### Apache Superset

To connect Apache Superset with Solr datasource, add the package to the requirements then create a database connection using the URL pattern shown above.

## Testing with Apache Superset

#### Requirements

* A Solr instance with a Parallel SQL supported up and running
* A Superset instance up and running with this package installed
* `pytest` >= 7.4.4 installed on the testing machine

#### Procedure

1. Change `conftest.py` as appropriate
2. Run `pytest`

## Resources
1. [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/index.html)
2. [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)
3. [PEP 249 – Python Database API Specification v2.0](https://peps.python.org/pep-0249/)
