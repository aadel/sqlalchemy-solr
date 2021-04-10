from sqlalchemy.sql import compiler


class SolrTypeCompiler(compiler.GenericTypeCompiler):
    def visit_ARRAY(self, type_, **kw):

        inner = self.process(type_.item_type)
        return inner
