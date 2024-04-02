from sqlalchemy import types
from sqlalchemy_solr.solrdbapi.array import ARRAY

type_map = {
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
