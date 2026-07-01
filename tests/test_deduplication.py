from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb
import pytest
from duckdb import DuckDBPyRelation

from iep.config import PATH_PACKAGE


@dataclass(kw_only=True, frozen=True, slots=True)
class _Case:
    identifier: str
    cluster: str


_CASES_FACILITY: Final[tuple[_Case, ...]] = (
    _Case(
        identifier="DE.EEA/16574.FACILITY",
        cluster="https://registry.gdi-de.org/id/de.st.lau.pf.anlagen-ied-euregistry/100787",
    ),
    _Case(
        identifier="IT.EEA/2007000625.FACILITY",
        cluster="IT.CAED/660503069.FACILITY",
    ),
    _Case(
        identifier="GB.EEA/EW_EA-360.FACILITY",
        cluster="UK.CAED/EW_EA-16879.FACILITY",
    ),
    _Case(
        identifier="IT.EEA/2007002367.FACILITY",
        cluster="IT.CAED/880442001.FACILITY",
    ),
    _Case(
        identifier="RO.EEA/RO5HD_318.FACILITY",
        cluster="RO.CAED/106HD0001.FACILITY",
    ),
    _Case(
        identifier="DE.EEA/14-10-46630660001.FACILITY",
        cluster="https://registry.gdi-de.org/id/de.sn.sax4inspire.pf/70015796",
    ),
    _Case(
        identifier="IT.EEA/2007000161.FACILITY",
        cluster="IT.CAED/380722001.FACILITY",
    ),
    _Case(
        identifier="PL.EEA/02C_002206.FACILITY",
        cluster="PL.MŚ/000000318.FACILITY",
    ),
    _Case(
        identifier="DE.EEA/17928.FACILITY",
        cluster="https://registry.gdi-de.org/id/de.st.lau.pf.anlagen-ied-euregistry/100276",
    ),
    _Case(
        identifier="PT.EEA/100002393.FACILITY",
        cluster="PT.CAED/PT.APA05765202.CI",
    ),
    _Case(
        identifier="RS.EEA/124088.FACILITY",
        cluster="RS.SEPA.NRIZ/FACILITY.000000035",
    ),
    # TODO: Facility_INSPIRE_IDs overlap temporally
    # _Case(
    #     identifier="HR.EEA/HR010288724.FACILITY",
    #     cluster="HR.CAED/000000019.FACILITY",
    # ),
)

_CASES_LCPMAPPING: Final[tuple[_Case, ...]] = (
    # _Case(
    #     identifier="RO.EEA/RO0160.FACILITY",
    # ),
    _Case(
        identifier="PL.EEA/PL0361.FACILITY",
        cluster="PL.MŚ/000000425.FACILITY",
    ),
    _Case(
        identifier="AT.EEA/AT0002.FACILITY",
        cluster="AT.EEA/20000.00051.FACILITY",
    ),
    _Case(
        identifier="AT.EEA/AT0084.FACILITY",
        # cluster="AT.CAED/9008390975220.FACILITY", # The unmapped LCP is labelled "Verbund" but it seems to report energy inputs for EVN https://www.gem.wiki/Duernrohr_power_station
        cluster="AT.EEA/20000.00106.FACILITY",  # Verbund thermal
    ),
    _Case(
        identifier="BG.EEA/BG0005.FACILITY",
        cluster="BG.CAED/017000006.FACILITY",
    ),
    _Case(
        identifier="LT.EEA/LT0032.FACILITY",
        cluster="LT.CAED/166451720.FACILITY",
    ),
    _Case(
        identifier="LT.EEA/LT0033.FACILITY",
        cluster="LT.CAED/166451720.FACILITY",
    ),
    _Case(
        identifier="LT.EEA/LT0126.FACILITY",
        cluster="LT.CAED/166451720.FACILITY",
    ),
    _Case(
        identifier="FR.EEA/FR0392.FACILITY",
        cluster="FR.EEA/059.06226.FACILITY",
    ),
)


@pytest.fixture(scope="session")
def deduplication_facility() -> DuckDBPyRelation:
    relation = duckdb.read_csv(
        Path(PATH_PACKAGE, "_input", "deduplication_facility.csv")
    )
    relation.create("deduplication_facility")
    return duckdb.table("deduplication_facility")


@pytest.fixture(scope="session")
def deduplication_lcpmapping() -> DuckDBPyRelation:
    relation = duckdb.read_csv(
        Path(PATH_PACKAGE, "_input", "deduplication_lcpmapping.csv")
    )
    relation.create("deduplication_lcp")
    return duckdb.table("deduplication_lcp")


# TODO: think about duplicates
@pytest.mark.xfail
def test_deduplication_facility_uniqueness(
    deduplication_facility: DuckDBPyRelation,
) -> None:
    duplicates = (
        deduplication_facility.select(
            "*, COUNT(*) OVER (PARTITION BY Facility_INSPIRE_ID) AS counts"
        )
        .filter("counts > 1")
        .fetchall()
    )
    assert not duplicates, (
        f"{len(duplicates)} duplicate Facility_INSPIRE_IDs:\n {duplicates[:20]}"
    )


def test_deduplication_lcpmapping_uniqueness(
    deduplication_lcpmapping: DuckDBPyRelation,
) -> None:
    duplicates = (
        deduplication_lcpmapping.select(
            "*, COUNT(*) OVER (PARTITION BY Facility_INSPIRE_ID) AS counts"
        )
        .filter("counts > 1")
        .fetchall()
    )
    assert not duplicates, (
        f"{len(duplicates)} duplicate Facility_INSPIRE_IDs:\n {duplicates[:20]}"
    )


@pytest.mark.parametrize("case", _CASES_FACILITY)
def test_deduplication_facility_inspire_id(
    case: _Case, deduplication_facility: DuckDBPyRelation
) -> None:
    expected = case.cluster
    actual = (
        deduplication_facility.filter(f"Facility_INSPIRE_ID = '{case.identifier}'")
        .select("Facility_INSPIRE_ID_cluster")
        .fetchone()
    )
    assert actual is not None
    assert actual[0] == expected


@pytest.mark.parametrize("case", _CASES_LCPMAPPING)
def test_deduplication_lcpmapping(
    case: _Case, deduplication_lcpmapping: DuckDBPyRelation
) -> None:
    expected = case.cluster
    actual = (
        deduplication_lcpmapping.filter(f"Facility_INSPIRE_ID = '{case.identifier}'")
        .select("Facility_INSPIRE_ID_cluster")
        .fetchone()
    )
    assert actual is not None
    assert actual[0] == expected
