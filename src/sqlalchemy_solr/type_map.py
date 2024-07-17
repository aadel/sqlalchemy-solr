import base64
from datetime import datetime

from sqlalchemy import types
from sqlalchemy_solr.solrdbapi import array
from sqlalchemy_solr.solrdbapi.array import ARRAY

NoneType = type(None)

metadata_type_map = {
    "binary": types.LargeBinary(),
    "boolean": types.Boolean(),
    "date": types.DateTime(),
    "pdate": types.DateTime(),
    "int": types.Integer(),
    "pint": types.Integer(),
    "long": types.BigInteger(),
    "plong": types.BigInteger(),
    "float": types.Float(),
    "pfloat": types.Float(),
    "double": types.REAL(),
    "pdouble": types.REAL(),
    "string": types.VARCHAR(),
    "text_general": types.Text(),
    "booleans": ARRAY(types.BOOLEAN()),
    "ints": ARRAY(types.Integer()),
    "pints": ARRAY(types.Integer()),
    "longs": ARRAY(types.BigInteger()),
    "plongs": ARRAY(types.BigInteger()),
    "floats": ARRAY(types.Float()),
    "pfloats": ARRAY(types.Float()),
    "doubles": ARRAY(types.REAL()),
    "pdoubles": ARRAY(types.REAL()),
    "strings": ARRAY(types.VARCHAR()),
}

# Used when field types cannot be determined using Solr metadata
native_type_map = {
    int: types.Integer(),
    float: types.Float(),
    str: types.VARCHAR(),
}

# Resultset conversion mapping
result_conversion_mapping = {
    types.LargeBinary: base64.b64decode,
    types.Boolean: bool,
    types.BigInteger: int,
    types.Integer: int,
    types.Float: float,
    types.REAL: float,
    types.DateTime: lambda x: datetime.fromisoformat(
        x[:-1]
    ),  # Remove Z for Python < 3.11
    types.VARCHAR: lambda x: x,
    types.Text: lambda x: x,
    array.ARRAY: lambda x: x,
    NoneType: lambda x: x,
}
