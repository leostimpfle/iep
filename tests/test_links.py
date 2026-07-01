from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb
import pytest
from duckdb import DuckDBPyRelation

from iep.config import PATH_INPUT


@dataclass(kw_only=True, frozen=True, slots=True)
class _LinkLcpPart:
    lcp_identifier: str
    installation_part_inspire_id: str


@dataclass(kw_only=True, frozen=True, slots=True)
class _LinkLcpFacility:
    lcp_identifier: str
    facility_inspire_id: str


_LINKS_LCP_PART: Final[tuple[_LinkLcpPart, ...]] = (
    _LinkLcpPart(
        lcp_identifier="DE0204",
        installation_part_inspire_id="https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/anl-2017-513000-500-0342658-0001g",
    ),
    _LinkLcpPart(
        lcp_identifier="UK0091",
        installation_part_inspire_id="UK.CAED/EW_EA-67-1.PART",
    ),
    _LinkLcpPart(
        lcp_identifier="PL0028",
        installation_part_inspire_id="PL.MŚ/000000019.PART",
    ),
    _LinkLcpPart(
        lcp_identifier="FR0105",
        installation_part_inspire_id="FR.CAED/6708.A.INSTALLATIONPART",
    ),
    _LinkLcpPart(
        lcp_identifier="UK0144",
        installation_part_inspire_id="UK.CAED/EW_EA-1089-6.PART",
    ),
    # _Link(
    #     lcp_identifier="BG0018",
    #     installation_part_inspire_id="BG.CAED/010300011.PART",
    # ),
    # _Link(
    #     lcp_identifier="DE0268",
    #     installation_part_inspire_id=
    # )
)

_LINKS_LCP_FACILITY: Final[tuple[_LinkLcpFacility, ...]] = (
    _LinkLcpFacility(
        lcp_identifier="DE0175",
        facility_inspire_id="https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/arb-2017-354012-300-0877384",
    ),
    _LinkLcpFacility(
        lcp_identifier="DE0431",
        facility_inspire_id="https://registry.gdi-de.org/id/de.bb.inspire.pf.eureg/45025564",
    ),
    _LinkLcpFacility(
        lcp_identifier="DE4108",
        facility_inspire_id="https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/arb-2017-362008-300-0326774",
    ),
    _LinkLcpFacility(
        lcp_identifier="FR0084", facility_inspire_id="FR.CAED/5892.FACILITY"
    ),
    _LinkLcpFacility(
        lcp_identifier="UK0578",
        facility_inspire_id="UK.SEPA/200000142.Facility",
    ),
    _LinkLcpFacility(
        lcp_identifier="EE0102",
        facility_inspire_id="EE.KAUR.TTR/75.FACILITY",
    ),
    _LinkLcpFacility(
        lcp_identifier="EE0100",
        facility_inspire_id="EE.KAUR.TTR/74.FACILITY",
    ),
    _LinkLcpFacility(
        lcp_identifier="PT0114",
        facility_inspire_id="PT.CAED/PT.APA05766202.CI",
    ),
    _LinkLcpFacility(
        lcp_identifier="BG0025",
        facility_inspire_id="BG.CAED/003000003.FACILITY",
    ),
    _LinkLcpFacility(
        lcp_identifier="DK0119",
        facility_inspire_id="DK.CAED/000104665.FACILITY",
    ),
)


@pytest.fixture(scope="session")
def links_lcp_part() -> DuckDBPyRelation:
    fn = "links_lcp_part"
    relation = duckdb.read_csv(Path(PATH_INPUT, f"{fn}.csv"))
    relation.create(fn)
    return duckdb.table(fn)


@pytest.fixture(scope="session")
def links_lcp_facility() -> DuckDBPyRelation:
    fn = "links_lcp_facility"
    relation = duckdb.read_csv(Path(PATH_INPUT, f"{fn}.csv"))
    relation.create(fn)
    return duckdb.table(fn)


def test_links_lcp_facility_unique(links_lcp_facility: DuckDBPyRelation) -> None:
    duplicates = (
        links_lcp_facility.select(
            "*, COUNT(*) OVER (PARTITION BY Unique_Plant_ID) AS counts"
        )
        .filter("counts > 1")
        .fetchall()
    )
    assert not duplicates, (
        f"{len(duplicates)} duplicate Unique_Plant_IDs:\n {duplicates[:20]}"
    )


def test_links_lcp_part_unique(links_lcp_part: DuckDBPyRelation) -> None:
    duplicates = (
        links_lcp_part.select(
            "*, COUNT(*) OVER (PARTITION BY Unique_Plant_ID) AS counts"
        )
        .filter("counts > 1")
        .fetchall()
    )
    assert not duplicates, (
        f"{len(duplicates)} duplicate Unique_Plant_IDs:\n {duplicates[:20]}"
    )


@pytest.mark.parametrize("link", _LINKS_LCP_PART)
def test_links_lcp_part(link: _LinkLcpPart, links_lcp_part: DuckDBPyRelation) -> None:
    expected = link.installation_part_inspire_id
    actual = (
        links_lcp_part.filter(f"Unique_Plant_ID = '{link.lcp_identifier}'")
        .select("Installation_Part_INSPIRE_ID")
        .fetchone()
    )
    assert actual is not None
    assert actual[0] == expected


@pytest.mark.parametrize("link", _LINKS_LCP_FACILITY)
def test_links_lcp_facility(
    link: _LinkLcpFacility, links_lcp_facility: DuckDBPyRelation
) -> None:
    expected = link.facility_inspire_id
    actual = (
        links_lcp_facility.filter(f"Unique_Plant_ID = '{link.lcp_identifier}'")
        .select("Facility_INSPIRE_ID")
        .fetchone()
    )
    assert actual is not None
    assert actual[0] == expected
