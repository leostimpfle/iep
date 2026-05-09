from functools import reduce
from pathlib import Path
from typing import Final, Protocol

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep
from iep.config import PATH_IEP
from iep.io import read_duckdb

_METADATA_NAME: Final[str] = "_VERSION_METADATA"


class Loader(Protocol):
    def __call__(
        self,
        *,
        version: str,
        reload: bool,
        connection: DuckDBPyConnection,
    ) -> DuckDBPyRelation: ...


VERSION: Final[str] = "Industrial_dataset_v_16_2026_02_16"


def load_metadata(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    return read_duckdb(
        fn=Path(PATH_IEP, version, "0a_DataCollectionMetadata_EUReg.csv"),
        dtypes={
            "fileId": "INTEGER",
            "envelopeUrl": "VARCHAR",
            "filename": "VARCHAR",
            "dateSubmitted": "TIMESTAMP",
            "dateReleased": "TIMESTAMP",
            "dateImported": "TIMESTAMP",
            "fileSHA256Hash": "VARCHAR",
            "countryCode": "VARCHAR",
            "reportingYear": "INTEGER",
            "obligation": "INTEGER",
        },
        connection=connection,
        reload=reload,
    )


def _stack(
    loader: Loader,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    versions = (p.stem for p in PATH_IEP.iterdir() if p.is_dir())
    data = (
        loader(version=version, reload=reload, connection=connection).select(
            f"*, '{version}' AS version"
        )
        for version in versions
    )
    return reduce(lambda left, right: left.union(right), data)


def _add_metadata(
    stacked: DuckDBPyRelation,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    try:
        metadata = connection.table(_METADATA_NAME)
    except duckdb.CatalogException:
        metadata = _stack(loader=load_metadata, reload=reload, connection=connection)
        metadata.create(_METADATA_NAME)
    return stacked.join(
        metadata.select(
            "version, fileId AS fileId_EUReg, dateSubmitted, reportingYear"
        ),
        condition="fileId_EUReg, version",
        how="left",
    )


def stack_versions(
    loader: Loader,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    stacked = _stack(loader=loader, reload=reload, connection=connection)
    stacked = _add_metadata(stacked, reload=reload, connection=connection)
    aggregate_by = {"reportingYear", "Parent_Site_INSPIRE_ID", "Facility_INSPIRE_ID"}
    order_by = {"reportingYear", "fileId_EUReg", "dateSubmitted", "version"}
    aggregate = [
        column for column in stacked.columns if column not in aggregate_by | order_by
    ]
    stacked = stacked.aggregate(
        f"""{", ".join(aggregate_by)},
        {", ".join(f"LAST({c} ORDER BY {', '.join(order_by)}) AS {c}" for c in aggregate)}"""
    )
    return stacked


if __name__ == "__main__":
    connection = duckdb.connect()
    facilities = stack_versions(
        loader=iep.facility.facility.load_facility, reload=False, connection=connection
    )
    details = iep.facility.details.load(connection=connection)
    function = iep.facility.function.load(connection=connection)
    function = stack_versions(loader=iep.facility.function.load, connection=connection)
    # details = stack_versions(
    #     loader=load_facility_details, reload=False, connection=connection
    # )
