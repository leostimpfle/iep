from dataclasses import dataclass
from typing import Final

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
    sanitised_energy_input_tj: float


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
)


@pytest.fixture
def raw() -> DuckDBPyRelation:
    return iep.part.energy_input._load_raw()


@pytest.fixture
def sanitised() -> DuckDBPyRelation:
    return iep.part.energy_input.load(sanitise=True)


_RANGE_DELTA: Final[tuple[int, int]] = (200, 300)


def test_count(raw: DuckDBPyRelation, sanitised: DuckDBPyRelation) -> None:
    delta = (
        raw.aggregate(
            "reportingYear, Installation_Part_INSPIRE_ID, SUM(energyInputTJ) AS raw"
        )
        .join(
            sanitised.aggregate(
                "reportingYear, Installation_Part_INSPIRE_ID, SUM(energyInputTJ) AS sanitised"
            ),
            condition="reportingYear, Installation_Part_INSPIRE_ID",
            how="outer",
        )
        .filter("ROUND(raw) != ROUND(sanitised)")
    )
    n_delta = delta.shape[0]
    assert n_delta > _RANGE_DELTA[0] and n_delta < _RANGE_DELTA[1]


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(case: _Case, sanitised: DuckDBPyRelation) -> None:
    test = sanitised.filter(
        f"""Installation_Part_INSPIRE_ID = '{case.installation_part}'
        AND reportingYear = {case.year}
        AND fuelInputCode = '{case.sanitised_fuel_input_code}'
        AND ROUND(energyInputTJ) = ROUND({case.sanitised_energy_input_tj})
        """
    )
    assert test.shape[0] == 1
