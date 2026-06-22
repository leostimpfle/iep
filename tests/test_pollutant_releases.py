from dataclasses import dataclass
from typing import Final

import duckdb
import pytest
from duckdb import DuckDBPyRelation

import iep


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    facility: str
    year: int
    pollutant_code: str
    medium: str
    sanitised_total_pollutant_quantity_kg: float
    raw_total_pollutant_quantity_kg: float


_CASES: Final[tuple[_Case, ...]] = (
    _Case(
        facility="IT.CAED/660503069.FACILITY",  # deduplicated from "IT.EEA/2007000625.FACILITY"
        year=2016,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=1_110_000_000,
        sanitised_total_pollutant_quantity_kg=111_000_000,
    ),
    _Case(
        facility="ES.CAED/003519000.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2007,
        raw_total_pollutant_quantity_kg=5_130_000_000,
        sanitised_total_pollutant_quantity_kg=5_130_000_000,
    ),
    _Case(
        facility="ES.CAED/003519000.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2008,
        raw_total_pollutant_quantity_kg=312_000_000,
        sanitised_total_pollutant_quantity_kg=312_000_000,
    ),
    _Case(
        facility="ES.CAED/003519000.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2009,
        raw_total_pollutant_quantity_kg=1_480_000_000,
        sanitised_total_pollutant_quantity_kg=1_480_000_000,
    ),
    # TODO: both appear to be wrong? Check LT_1 in EU ETS (https://www.euets.info/installation/LT_1)
    # _Case(
    #     facility="LT.CAED/153009143.FACILITY",
    #     pollutant_code="CO2",
    #     medium="AIR",
    #     year=2018,
    #     raw_total_pollutant_quantity_kg=2_370_000,
    #     sanitised_total_pollutant_quantity_kg=2_370_000_000,
    # ),
    _Case(
        facility="CZ.MZP.T805/CZ93379263.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2020,
        sanitised_total_pollutant_quantity_kg=52_520_000,
        raw_total_pollutant_quantity_kg=52_520,
    ),
    _Case(
        facility="DK.CAED/000082948.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2024,
        sanitised_total_pollutant_quantity_kg=288_000_000,
        raw_total_pollutant_quantity_kg=288_000,
    ),
    _Case(
        facility="ES.CAED/002112000.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2021,
        sanitised_total_pollutant_quantity_kg=210_000_000,
        raw_total_pollutant_quantity_kg=210_000_000,
    ),
    _Case(
        facility="IS.CAED/520303-4210",
        pollutant_code="CO2",
        medium="AIR",
        year=2020,
        sanitised_total_pollutant_quantity_kg=528_000_000,
        raw_total_pollutant_quantity_kg=528_000,
    ),
    _Case(
        facility="PL.MŚ/000003652.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2020,
        sanitised_total_pollutant_quantity_kg=933_000_000,
        raw_total_pollutant_quantity_kg=933_000_000,
    ),
    _Case(
        facility="CZ.MZP.E531/CZ27995052.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2021,
        sanitised_total_pollutant_quantity_kg=628_427_000,
        raw_total_pollutant_quantity_kg=628_427,
    ),
    _Case(
        facility="CZ.MZP.U423/CZ15080054.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2022,
        sanitised_total_pollutant_quantity_kg=191_734_000,
        raw_total_pollutant_quantity_kg=191_734,
    ),
    _Case(
        facility="DK.CAED/000105331.FACILITY",
        year=2018,
        pollutant_code="CO2",
        medium="AIR",
        sanitised_total_pollutant_quantity_kg=81_443_000,
        raw_total_pollutant_quantity_kg=81_443,
    ),
    _Case(
        facility="FR.CAED/13773.FACILITY",
        year=2018,
        pollutant_code="CO2",
        medium="AIR",
        sanitised_total_pollutant_quantity_kg=353_000_000,
        raw_total_pollutant_quantity_kg=353_000_000,
    ),
    _Case(
        facility="NL.RIVM/000064335.FACILITY",
        year=2018,
        pollutant_code="CO2",
        medium="AIR",
        sanitised_total_pollutant_quantity_kg=146_000_000,
        raw_total_pollutant_quantity_kg=146_000,
    ),
    _Case(
        facility="LT.CAED/156667399.FACILITY",
        year=2010,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=1_370_000_000,
        sanitised_total_pollutant_quantity_kg=1_370_000_000,
    ),
    _Case(
        facility="LT.CAED/156667399.FACILITY",
        year=2011,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=112_000_000,
        sanitised_total_pollutant_quantity_kg=1_120_000_000,
    ),
    _Case(
        facility="LT.CAED/156667399.FACILITY",
        year=2012,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=212_000_000,
        sanitised_total_pollutant_quantity_kg=2_120_000_000,
    ),
    _Case(
        facility="LT.CAED/156667399.FACILITY",
        year=2013,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=205_000_000,
        sanitised_total_pollutant_quantity_kg=2_050_000_000,
    ),
    _Case(
        facility="LT.CAED/156667399.FACILITY",
        year=2019,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=2_610_000_000,
        sanitised_total_pollutant_quantity_kg=2_610_000_000,
    ),
    _Case(
        facility="HR.CAED/000000019.FACILITY",
        year=2017,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=306_000_000,
        sanitised_total_pollutant_quantity_kg=306_000_000,
    ),
    _Case(
        facility="FR.CAED/6388.FACILITY",
        year=2020,
        pollutant_code="CO2",
        medium="AIR",
        raw_total_pollutant_quantity_kg=104_000_000_000,
        sanitised_total_pollutant_quantity_kg=104_000_000,
    ),
    # _Case(
    #     facility="RO.CAED/101DJ0001.FACILITY",
    #     year=2014,
    #     pollutant_code="CO2",
    #     medium="AIR",
    #     raw_total_pollutant_quantity_kg=2_380_000_000,
    #     sanitised_total_pollutant_quantity_kg=2_380_000_000,
    # ),
    # _Case(
    #     facility="UK.CAED/EW_EA-67.FACILITY",
    #     year=2019,
    #     pollutant_code="CO2EXCLBIOMASS",
    #     medium="AIR",
    #     raw_total_pollutant_quantity_kg=12_300_000_000,
    #     sanitised_total_pollutant_quantity_kg=690_000,
    # ),
)


@pytest.fixture(scope="session")
def raw() -> DuckDBPyRelation:
    relation = iep.facility.pollutant_release._load_raw()
    relation.create("pollutant_releases_raw")
    return duckdb.table("pollutant_releases_raw")


@pytest.fixture(scope="session")
def deduplicated() -> DuckDBPyRelation:
    relation = iep.facility.pollutant_release.load(deduplicate=True, sanitise=False)
    relation.create("pollutant_releases_deduplicated")
    return duckdb.table("pollutant_releases_deduplicated")


@pytest.fixture(scope="session")
def sanitised() -> DuckDBPyRelation:
    relation = iep.facility.pollutant_release.load(deduplicate=True, sanitise=True)
    relation.create("pollutant_releases_sanitised")
    return duckdb.table("pollutant_releases_sanitised")


def test_count(deduplicated: DuckDBPyRelation, sanitised: DuckDBPyRelation) -> None:
    range_delta: Final[tuple[int, int]] = (100, 250)
    delta = (
        deduplicated.aggregate(
            "reportingYear, Facility_INSPIRE_ID, pollutantCode, medium, SUM(totalPollutantQuantityKg) AS raw"
        )
        .join(
            sanitised.aggregate(
                "reportingYear, Facility_INSPIRE_ID, pollutantCode, medium, SUM(totalPollutantQuantityKg) AS sanitised"
            ),
            condition="reportingYear, Facility_INSPIRE_ID, pollutantCode, medium",
            how="outer",
        )
        .filter("ROUND(raw) != ROUND(sanitised)")
    )
    n_delta = delta.shape[0]
    assert n_delta > range_delta[0] and n_delta < range_delta[1]


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(case: _Case, sanitised: DuckDBPyRelation) -> None:
    actual = (
        sanitised.filter(
            f"""Facility_INSPIRE_ID = '{case.facility}'
            AND reportingYear = {case.year}
            AND pollutantCode = '{case.pollutant_code}'
            AND medium = '{case.medium}'
            """
        )
        .select("totalPollutantQuantityKg")
        .fetchall()
    )
    assert actual is not None and len(actual) == 1
    assert actual[0][0] == pytest.approx(
        case.sanitised_total_pollutant_quantity_kg, abs=1
    )
