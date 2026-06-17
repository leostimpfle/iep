from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb
import pytest
from duckdb import DuckDBPyRelation

from iep.config import PATH_PACKAGE


@dataclass(kw_only=True, frozen=True, slots=True)
class _Link:
    lcp_identifier: str
    installation_part_inspire_id: str


_LINKS: Final[tuple[_Link, ...]] = (
    _Link(
        lcp_identifier="DE0204",
        installation_part_inspire_id="https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/anl-2017-513000-500-0342658-0001g",
    ),
)


@pytest.fixture(scope="session")
def deduplication_facility() -> DuckDBPyRelation:
    relation = duckdb.read_csv(Path(PATH_PACKAGE, "_input", "links_lcp.csv"))
    relation.create("links_lcp")
    return duckdb.table("links_lcp")


@pytest.mark.parametrize("link", _LINKS)
def test_deduplication_facility_inspire_id(
    link: _Link, deduplication_facility: DuckDBPyRelation
) -> None:
    expected = link.installation_part_inspire_id
    actual = (
        deduplication_facility.filter(f"Unique_Plant_ID = '{link.lcp_identifier}'")
        .select("Installation_Part_INSPIRE_ID")
        .fetchone()
    )
    assert actual is not None
    assert actual[0] == expected
