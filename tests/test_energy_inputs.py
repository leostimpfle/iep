from dataclasses import dataclass
from typing import Final

import pytest
from duckdb import DuckDBPyRelation

import iep


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    installation_part: str
    year: int
    fuel_input_code: str
    energy_input_tj: float


_CASES: Final[tuple[_Case, ...]] = (
    _Case(
        installation_part="IT.CAED/100401001.PART",
        year=2019,
        fuel_input_code="NaturalGas",
        energy_input_tj=13_022,
    ),
    _Case(
        installation_part="IT.CAED/570162008.PART",
        year=2019,
        fuel_input_code="NaturalGas",
        energy_input_tj=212.615,
    ),
    _Case(
        installation_part="AT.CAED/9008390317877.PART",
        year=2016,
        fuel_input_code="Biomass",
        energy_input_tj=2_590,
    ),
    _Case(
        installation_part="AT.CAED/9008390317877.PART",
        year=2017,
        fuel_input_code="Biomass",
        energy_input_tj=2_520,
    ),
    _Case(
        installation_part="ES.CAED/002112001.PART",
        year=2018,
        fuel_input_code="NaturalGas",
        energy_input_tj=0.54,
    ),
    _Case(
        installation_part="ES.CAED/002112001.PART",
        year=2021,
        fuel_input_code="NaturalGas",
        energy_input_tj=1561,
    ),
)


@pytest.fixture
def sanitised() -> DuckDBPyRelation:
    return iep.part.energy_input.load(sanitise=True)


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(case: _Case, sanitised: DuckDBPyRelation) -> None:
    test = sanitised.filter(
        f"""Installation_Part_INSPIRE_ID = '{case.installation_part}'
        AND reportingYear = {case.year}
        AND fuelInputCode = '{case.fuel_input_code}'
        AND ROUND(energyInputTJ) = ROUND({case.energy_input_tj})
        """
    )
    assert test.shape[0] == 1
