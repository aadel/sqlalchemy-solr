from sqlalchemy import types

from sqlalchemy_solr.solrdbapi.array import ARRAY

type_map = {
    "binary": types.LargeBinary(),
    "boolean": types.Boolean(),
    "pdate": types.DateTime(),
    "pint": types.Integer(),
    "plong": types.BigInteger(),
    "pfloat": types.Float(),
    "pdouble": types.REAL(),
    "string": types.VARCHAR(),
    "text_general": types.Text(),
    "booleans": ARRAY(types.BOOLEAN()),
    "pints": ARRAY(types.Integer()),
    "plongs": ARRAY(types.BigInteger()),
    "pfloats": ARRAY(types.Float()),
    "pdoubles": ARRAY(types.REAL()),
    "strings": ARRAY(types.VARCHAR()),
}
