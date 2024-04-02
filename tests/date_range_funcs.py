from dataclasses import dataclass

import pytest
from sqlalchemy import and_
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy_solr.admin.solr_spec import SolrSpec


@dataclass
class Parameters:
    engine: any
    t: Table
    lower_bound: str
    upper_bound: str
    lower_bound_iso: str
    upper_bound_iso: str
    select_statements: list[str]


def date_range_parameters_1(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] >= parameters.lower_bound,
                parameters.t.columns["ORDERDATE_dt"] <= parameters.upper_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_2(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] > parameters.lower_bound,
                parameters.t.columns["ORDERDATE_dt"] <= parameters.upper_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_3(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] >= parameters.lower_bound,
                parameters.t.columns["ORDERDATE_dt"] < parameters.upper_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_4(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] > parameters.lower_bound,
                parameters.t.columns["ORDERDATE_dt"] < parameters.upper_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_5(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] <= parameters.upper_bound,
                parameters.t.columns["ORDERDATE_dt"] >= parameters.lower_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_6(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] < parameters.upper_bound,
                parameters.t.columns["ORDERDATE_dt"] >= parameters.lower_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_7(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] <= parameters.upper_bound,
                parameters.t.columns["ORDERDATE_dt"] > parameters.lower_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_8(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(
            and_(
                parameters.t.columns["ORDERDATE_dt"] < parameters.upper_bound,
                parameters.t.columns["ORDERDATE_dt"] > parameters.lower_bound,
            )
        )
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_9(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(parameters.t.columns["ORDERDATE_dt"] > parameters.lower_bound)
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_10(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(parameters.t.columns["ORDERDATE_dt"] <= parameters.upper_bound)
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_11(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(parameters.t.columns["ORDERDATE_dt"] >= parameters.lower_bound)
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


def date_range_parameters_12(settings, parameters, releases):
    solr_spec = SolrSpec(settings["SOLR_BASE_URL"])

    if solr_spec.spec()[0] not in releases:
        pytest.skip(
            reason=f"Solr spec version {solr_spec} not compatible with the current test"
        )

    qry = (
        (select(parameters.t.c.CITY_s).select_from(parameters.t))
        .where(parameters.t.columns["ORDERDATE_dt"] < parameters.upper_bound)
        .limit(1)
    )

    with parameters.engine.connect() as connection:
        result = connection.execute(qry)

    return result


date_range_funcs = [lambda x: ()]
date_range_funcs.append(date_range_parameters_1)
date_range_funcs.append(date_range_parameters_2)
date_range_funcs.append(date_range_parameters_3)
date_range_funcs.append(date_range_parameters_4)
date_range_funcs.append(date_range_parameters_5)
date_range_funcs.append(date_range_parameters_6)
date_range_funcs.append(date_range_parameters_7)
date_range_funcs.append(date_range_parameters_8)
date_range_funcs.append(date_range_parameters_9)
date_range_funcs.append(date_range_parameters_10)
date_range_funcs.append(date_range_parameters_11)
date_range_funcs.append(date_range_parameters_12)
