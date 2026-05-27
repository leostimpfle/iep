from dataclasses import dataclass
from typing import Final

import pytest
from duckdb import DuckDBPyRelation

import iep


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    facility: str
    pollutant_code: str
    medium: str
    year: int
    is_error: bool


_CASES: Final[tuple[_Case, ...]] = (
    _Case(
        facility="ES.CAED/003519000.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2008,
        is_error=False,
    ),
    _Case(
        facility="LT.CAED/153009143.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2018,
        is_error=True,
    ),
    _Case(
        facility="CZ.MZP.T805/CZ93379263.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2020,
        is_error=True,
    ),
    _Case(
        facility="DK.CAED/000082948.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2023,
        is_error=True,
    ),
    _Case(
        facility="ES.CAED/002112000.FACILITY",
        pollutant_code="CO2",
        medium="AIR",
        year=2021,
        is_error=False,
    ),
)


@pytest.fixture
def raw() -> DuckDBPyRelation:
    return iep.facility.pollutant_release.load(
        deduplicate=False, sanitise=False, interpolate=False
    )


@pytest.fixture
def sanitised() -> DuckDBPyRelation:
    return iep.facility.pollutant_release.load(
        deduplicate=False, sanitise=True, interpolate=False
    )


@pytest.mark.parametrize("case", _CASES)
def test_sanitise(
    case: _Case, raw: DuckDBPyRelation, sanitised: DuckDBPyRelation
) -> None:
    columns: list[str] = [
        "reportingYear",
        "Facility_INSPIRE_ID",
        "pollutantCode",
        "medium",
    ]
    test = (
        raw.select(f"{', '.join(columns)}, totalPollutantQuantityKg AS raw")
        .join(
            sanitised.select(
                f"{', '.join(columns)}, totalPollutantQuantityKg AS sanitised",
            ),
            condition=", ".join(columns),
            how="inner",
        )
        .filter(
            f"""Facility_INSPIRE_ID = '{case.facility}'
            AND reportingYear = {case.year}
            AND pollutantCode = '{case.pollutant_code}'
            AND medium = '{case.medium}'
            AND raw {"!=" if case.is_error else "="} sanitised 
            """
        )
    )
    assert test.shape[0] == 1
