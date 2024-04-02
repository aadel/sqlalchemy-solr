from sqlalchemy import types


class ARRAY(types.ARRAY):
    def __init__(self, item_type):
        super().__init__(item_type)

    def get_col_spec(self, **kw):  # pylint: disable=unused-argument
        return f"Type({self.item_type})"

    def bind_processor(self, dialect):
        def process(value):
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process
