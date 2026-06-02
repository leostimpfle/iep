from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb
import pytest
from duckdb import DuckDBPyRelation

from iep.config import PATH_PACKAGE


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    facility_inspire_id: str
    facility_inspire_id_cluster: str


_CASES: Final[tuple[_Case, ...]] = (
    _Case(
        facility_inspire_id="DE.EEA/16574.FACILITY",
        facility_inspire_id_cluster="https://registry.gdi-de.org/id/de.st.lau.pf.anlagen-ied-euregistry/100787",
    ),
    _Case(
        facility_inspire_id="IT.EEA/2007000625.FACILITY",
        facility_inspire_id_cluster="IT.CAED/660503069.FACILITY",
    ),
    _Case(
        facility_inspire_id="GB.EEA/EW_EA-360.FACILITY",
        facility_inspire_id_cluster="UK.CAED/EW_EA-16879.FACILITY",
    ),
    _Case(
        facility_inspire_id="IT.EEA/2007002367.FACILITY",
        facility_inspire_id_cluster="IT.CAED/880442001.FACILITY",
    ),
    _Case(
        facility_inspire_id="RO.EEA/RO5HD_318.FACILITY",
        facility_inspire_id_cluster="RO.CAED/106HD0001.FACILITY",
    ),
    _Case(
        facility_inspire_id="DE.EEA/14-10-46630660001.FACILITY",
        facility_inspire_id_cluster="https://registry.gdi-de.org/id/de.sn.sax4inspire.pf/70015796",
    ),
    _Case(
        facility_inspire_id="IT.EEA/2007000161.FACILITY",
        facility_inspire_id_cluster="IT.CAED/380722001.FACILITY",
    ),
    _Case(
        facility_inspire_id="PL.EEA/02C_002206.FACILITY",
        facility_inspire_id_cluster="PL.MŚ/000000318.FACILITY",
    ),
    _Case(
        facility_inspire_id="DE.EEA/17928.FACILITY",
        facility_inspire_id_cluster="https://registry.gdi-de.org/id/de.st.lau.pf.anlagen-ied-euregistry/100276",
    ),
    _Case(
        facility_inspire_id="PT.EEA/100002393.FACILITY",
        facility_inspire_id_cluster="PT.CAED/PT.APA05765202.CI",
    ),
    _Case(
        facility_inspire_id="RS.EEA/124088.FACILITY",
        facility_inspire_id_cluster="RS.SEPA.NRIZ/FACILITY.000000035",
    ),
)


@pytest.fixture(scope="session")
def deduplication() -> DuckDBPyRelation:
    relation = duckdb.read_csv(Path(PATH_PACKAGE, "facility", "deduplication.csv"))
    relation.create("deduplication")
    return duckdb.table("deduplication")


@pytest.mark.parametrize("case", _CASES)
def test_deduplication(case: _Case, deduplication: DuckDBPyRelation) -> None:
    expected = case.facility_inspire_id_cluster
    actual = (
        deduplication.filter(f"Facility_INSPIRE_ID = '{case.facility_inspire_id}'")
        .select("Facility_INSPIRE_ID_cluster")
        .fetchone()
    )
    assert actual is not None
    assert actual[0] == expected
