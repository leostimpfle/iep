from dataclasses import dataclass
from typing import Final

import duckdb
import pytest
from duckdb import DuckDBPyRelation

import iep


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    installation_part: str
    year: int
    raw_fuel_input_code: str
    sanitised_fuel_input_code: str
    raw_energy_input_tj: float
    sanitised_energy_input_tj: float | None


_CASES: Final[tuple[_Case, ...]] = (
    _Case(
        installation_part="IT.CAED/100401001.PART",
        year=2019,
        raw_fuel_input_code="NaturalGas",
        sanitised_fuel_input_code="NaturalGas",
        raw_energy_input_tj=130_221,
        sanitised_energy_input_tj=13_022,
    ),
    _Case(
        installation_part="IT.CAED/570162008.PART",
        year=2019,
        raw_fuel_input_code="NaturalGas",
        sanitised_fuel_input_code="NaturalGas",
        raw_energy_input_tj=212_615,
        sanitised_energy_input_tj=212.615,
    ),
    _Case(
        installation_part="AT.CAED/9008390317877.PART",
        year=2016,
        raw_fuel_input_code="OtherSolidFuels",
        sanitised_fuel_input_code="Biomass",
        raw_energy_input_tj=2.59,
        sanitised_energy_input_tj=2_590,
    ),
    _Case(
        installation_part="AT.CAED/9008390317877.PART",
        year=2017,
        raw_fuel_input_code="OtherSolidFuels",
        sanitised_fuel_input_code="Biomass",
        raw_energy_input_tj=2.52,
        sanitised_energy_input_tj=2_520,
    ),
    _Case(
        installation_part="ES.CAED/002112001.PART",
        year=2018,
        raw_fuel_input_code="NaturalGas",
        sanitised_fuel_input_code="NaturalGas",
        raw_energy_input_tj=0.54,
        sanitised_energy_input_tj=0.54,
    ),
    _Case(
        installation_part="ES.CAED/002112001.PART",
        year=2021,
        raw_fuel_input_code="NaturalGas",
        sanitised_fuel_input_code="NaturalGas",
        raw_energy_input_tj=1561,
        sanitised_energy_input_tj=1561,
    ),
    _Case(
        installation_part="NL.RIVM/202419001.PART",
        year=2022,
        raw_fuel_input_code="LiquidFuels",
        sanitised_fuel_input_code="LiquidFuels",
        raw_energy_input_tj=2_885.988,
        sanitised_energy_input_tj=28.85988,
    ),
    _Case(
        installation_part="ES.CAED/002928000.PART",
        year=2018,
        raw_fuel_input_code="Coal",
        sanitised_fuel_input_code="Coal",
        raw_energy_input_tj=13.63,
        sanitised_energy_input_tj=1_363,
    ),
    _Case(
        installation_part="ES.CAED/002928001.PART",
        year=2018,
        raw_fuel_input_code="Coal",
        sanitised_fuel_input_code="Coal",
        raw_energy_input_tj=26.545,
        sanitised_energy_input_tj=26_545,
    ),
    _Case(
        installation_part="AT.CAED/9008390477526.PART",
        year=2020,
        raw_fuel_input_code="Biomass",
        sanitised_fuel_input_code="Biomass",
        raw_energy_input_tj=0.0,
        sanitised_energy_input_tj=None,
    ),
    _Case(
        installation_part="PT.CAED/PT.APA06042882.EQUIP",
        year=2019,
        raw_fuel_input_code="Biomass",
        sanitised_fuel_input_code="Biomass",
        raw_energy_input_tj=0.0,
        sanitised_energy_input_tj=None,
    ),
    # _Case(
    #     installation_part="PT.CAED/PT.APA05779642.EQUIP",
    #     year=2021,
    #     raw_fuel_input_code="NaturalGas",
    #     sanitised_fuel_input_code="NaturalGas",
    #     raw_energy_input_tj=32.33272177,
    #     sanitised_energy_input_tj=32.33272177,
    # ),
    # _Case(
    #     installation_part="PT.CAED/PT.APA06042862.EQUIP",
    #     year=2021,
    #     raw_fuel_input_code="NaturalGas",
    #     sanitised_fuel_input_code="NaturalGas",
    #     raw_energy_input_tj=23.05547423,
    #     sanitised_energy_input_tj=23.05547423,
    # ),
    # _Case(
    #     installation_part="PT.CAED/PT.APA06042862.EQUIP",
    #     year=2022,
    #     raw_fuel_input_code="NaturalGas",
    #     sanitised_fuel_input_code="NaturalGas",
    #     raw_energy_input_tj=0.480426606268,
    #     sanitised_energy_input_tj=0.480426606268,
    # ),
    # TODO: Installation_Part emissions also zero; need Facility to check
    # _Case(
    #     installation_part="ES.CAED/003378000.PART",
    #     year=2021,
    #     raw_fuel_input_code="Biomass",
    #     sanitised_fuel_input_code="Biomass",
    #     raw_energy_input_tj=0.0,
    #     sanitised_energy_input_tj=None,
    # ),
    # TODO: 2016 reporting incorrectly groups Biomass and LiquidFuels; probably no way to fix this programatically
    # _Case(
    #     installation_part="SE.CAED/10000064.Part",
    #     year=2016,
    #     raw_fuel_input_code="LiquidFuels",
    #     sanitised_fuel_input_code="LiquidFuels",
    #     raw_energy_input_tj=4_236.46,
    #     sanitised_energy_input_tj=-9999,
    # ),
    # TODO: Scaling doesn't work because `UK.CAED/EW_EA-13608-2.PART` reports 0.0 in 2016
    # _Case(
    #     installation_part="UK.CAED/EW_EA-13608-2.PART",
    #     year=2016,
    #     raw_fuel_input_code="OtherGases",
    #     sanitised_fuel_input_code="BlastFurnaceGas",
    #     raw_energy_input_tj=0.0,
    #     sanitised_energy_input_tj="3_000",
    # ),
)


@pytest.fixture(scope="session")
def raw() -> DuckDBPyRelation:
    relation = iep.part.energy_input._load_raw()
    relation.create("energy_inputs_raw")
    return duckdb.table("energy_inputs_raw")


@pytest.fixture(scope="session")
def sanitised() -> DuckDBPyRelation:
    relation = iep.part.energy_input.load(sanitise=True)
    relation.create("energy_inputs_sanitised")
    return duckdb.table("energy_inputs_sanitised")


def test_count(raw: DuckDBPyRelation, sanitised: DuckDBPyRelation) -> None:
    range_delta: Final[tuple[int, int]] = (700, 800)
    raw_agg = raw.aggregate(
        "reportingYear, Installation_Part_INSPIRE_ID, SUM(energyInputTJ) AS raw"
    )
    sanitised_agg = sanitised.aggregate(
        "reportingYear, Installation_Part_INSPIRE_ID, SUM(energyInputTJ) AS sanitised"
    )
    delta = raw_agg.join(
        sanitised_agg,
        condition="reportingYear, Installation_Part_INSPIRE_ID",
        how="outer",
    ).filter("ROUND(raw) != ROUND(sanitised)")
    n_delta = delta.shape[0]
    assert n_delta > range_delta[0] and n_delta < range_delta[1]


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(case: _Case, sanitised: DuckDBPyRelation) -> None:
    actual = (
        sanitised.filter(
            f"""Installation_Part_INSPIRE_ID = '{case.installation_part}'
        AND reportingYear = {case.year}
        AND fuelInputCode = '{case.sanitised_fuel_input_code}'
        """
        )
        .select("energyInputTJ")
        .fetchall()
    )
    assert actual is not None and len(actual) == 1
    if case.sanitised_energy_input_tj is None:
        assert actual[0][0] is None
    else:
        assert round(actual[0][0]) == round(case.sanitised_energy_input_tj)
