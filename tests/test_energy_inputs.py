from dataclasses import dataclass
from typing import Final

import pytest
from duckdb import DuckDBPyRelation

import iep


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    installation_part: str
    fuel_input_code: str
    year: int
    is_error: bool


_CASES: Final[tuple[_Case, ...]] = (
    _Case(
        installation_part="IT.CAED/100401001.PART",
        fuel_input_code="NaturalGas",
        year=2019,
        is_error=True,
    ),
    _Case(
        installation_part="IT.CAED/570162008.PART",
        fuel_input_code="NaturalGas",
        year=2019,
        is_error=True,
    ),
)


@pytest.fixture
def raw() -> DuckDBPyRelation:
    return iep.part.energy_input.load(sanitise=False)


@pytest.fixture
def sanitised() -> DuckDBPyRelation:
    return iep.part.energy_input.load(sanitise=True)


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(
    case: _Case, raw: DuckDBPyRelation, sanitised: DuckDBPyRelation
) -> None:
    columns: list[str] = [
        "reportingYear",
        "Installation_Part_INSPIRE_ID",
        "fuelInputCode",
    ]
    test = (
        raw.select(f"{', '.join(columns)}, energyInputTJ AS raw")
        .join(
            sanitised.select(
                f"{', '.join(columns)}, energyInputTj AS sanitised",
            ),
            condition=", ".join(columns),
            how="inner",
        )
        .filter(
            f"""Installation_Part_INSPIRE_ID = '{case.installation_part}'
            AND reportingYear = {case.year}
            AND fuelInputCode = '{case.fuel_input_code}'
            AND raw {"!=" if case.is_error else "="} sanitised 
            """
        )
    )
    assert test.shape[0] == 1
