from sqlalchemy.sql import compiler


class SolrTypeCompiler(compiler.GenericTypeCompiler):
    # pylint: disable=too-few-public-methods
    def visit_ARRAY(self, type_, **kw):     # pylint: disable=invalid-name

        inner = self.process(type_.item_type)
        return inner
