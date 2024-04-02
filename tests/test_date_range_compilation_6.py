from tests.date_range_funcs import date_range_funcs

releases = [6, 7, 8]


class TestDateRangeCompilation:

    def test_solr_date_range_compilation_1(self, settings, parameters):
        result = date_range_funcs[1](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '["
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_2(self, settings, parameters):
        result = date_range_funcs[2](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '{"
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_3(self, settings, parameters):
        result = date_range_funcs[3](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '["
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "}'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_4(self, settings, parameters):
        result = date_range_funcs[4](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[0]
            + "AND sales_test_.`ORDERDATE_dt` = '{"
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "}'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_5(self, settings, parameters):
        result = date_range_funcs[5](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'["
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "]' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_6(self, settings, parameters):
        result = date_range_funcs[6](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'["
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_7(self, settings, parameters):
        result = date_range_funcs[7](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'{"
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "]' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_8(self, settings, parameters):
        result = date_range_funcs[8](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'{"
            + parameters.lower_bound_iso
            + " TO "
            + parameters.upper_bound_iso
            + "}' AND TRUE\n LIMIT ?"
        )

    def test_solr_date_range_compilation_9(self, settings, parameters):
        result = date_range_funcs[9](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'{"
            + parameters.lower_bound_iso
            + " TO "
            + "*"
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_10(self, settings, parameters):
        result = date_range_funcs[10](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'["
            + "*"
            + " TO "
            + parameters.upper_bound_iso
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_11(self, settings, parameters):
        result = date_range_funcs[11](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'["
            + parameters.lower_bound_iso
            + " TO "
            + "*"
            + "]'\n LIMIT ?"
        )

    def test_solr_date_range_compilation_12(self, settings, parameters):
        result = date_range_funcs[12](settings, parameters, releases)

        assert (
            result.context.statement
            == parameters.select_statements[1]
            + "'["
            + "*"
            + " TO "
            + parameters.upper_bound_iso
            + "}'\n LIMIT ?"
        )
