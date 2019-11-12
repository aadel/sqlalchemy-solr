# Apache Solr dialect for SQLAlchemy.
---
A dialect for Apache Solr that can be used with Apache Superset.

https://superset.incubator.apache.org

## Installation
Installing the dialect is straightforward.  Simply:
1.  Clone or download this repository
2.  Navigate to the directory where you cloned the repo
3.  Run the python `setup.py` to install

Examples are shown below
```
git clone https://github.com/aadel/sqlalchemy-solr
cd sqlalchemy-solr
python3 setup.py install 

```

## Usage
To use Solr with SQLAlchemy, the following URL pattern can be used:

```
solr://<host>:<port>/solr/<collection>
```