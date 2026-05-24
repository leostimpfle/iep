from dataclasses import dataclass
from typing import Final

import pytest
from duckdb import DuckDBPyRelation

import iep


@dataclass(kw_only=True, frozen=True, slots=True)
class _TestCase:
    installation_part: str
    fuel_input_code: str
    year: int


_CASES: Final[tuple[_TestCase, ...]] = (
    _TestCase(
        installation_part="IT.CAED/100401001.PART",
        fuel_input_code="NaturalGas",
        year=2019,
    ),
    _TestCase(
        installation_part="IT.CAED/570162008.PART",
        fuel_input_code="NaturalGas",
        year=2019,
    ),
)


@pytest.fixture
def energy_inputs() -> DuckDBPyRelation:
    return iep.part.energy_input.load(balance=True, sanitise=True)


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(case: _TestCase, energy_inputs: DuckDBPyRelation) -> None:
    is_jump = energy_inputs.select(
        """*,
        LAG(energyInputTJ) OVER (
            PARTITION BY Installation_Part_INSPIRE_ID, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
            ORDER BY reportingYear
        ) AS lagged,
        CASE
            WHEN energyInputTJ > 0.0 AND lagged > 0.0
            THEN LOG10(energyInputTJ) - LOG10(lagged)
        END AS log_delta
        """
    ).filter(
        f"""Installation_Part_INSPIRE_ID = '{case.installation_part}'
        AND fuelInputCode = '{case.fuel_input_code}'
        AND reportingYear = {case.year}
        AND log_delta > 0.5
        """
    )
    assert is_jump.shape[0] == 0
