from enum import IntEnum
from pathlib import Path
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep.facility.facility
from iep.config import PATH_IEP, PATH_PACKAGE
from iep.io import NA_VALUES, read_duckdb
from iep.versions import VERSION


class Level(IntEnum):
    Site = 1
    Facility = 2
    Installation = 3
    Installation_Part = 4


def _get_columns(level: Level, include_name: bool = True) -> list[str]:
    columns: list[str] = [f"{level.name}_INSPIRE_ID"]
    if level > Level.Site:
        columns.append(f"Parent_{Level(level.value - 1).name}_INSPIRE_ID")
    if include_name:
        columns.append("nameOfFeature")
    return columns


def _load_site(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "1_ProductionSite"
    data = read_duckdb(
        fn=Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "Site_INSPIRE_ID": "VARCHAR",
            "ProductionSite_thematicId": "VARCHAR",
            "ProductionSite_thematicIdScheme": "VARCHAR",
            "pointGeometryLat": "DOUBLE",
            "pointGeometryLon": "DOUBLE",
            "nameOfFeature": "VARCHAR",
            "countryCode": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )
    return data


def load(
    include_name: bool = True,
    case_sensitive: bool = True,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    frames: Final[dict[Level, DuckDBPyRelation]] = {
        Level.Site: _load_site(reload=reload, connection=connection),
        Level.Facility: iep.facility.facility._load_raw(
            reload=reload, connection=connection
        ),
        Level.Installation: iep.installation.installation.load(
            reload=reload, connection=connection
        ),
        Level.Installation_Part: iep.part.part.load(
            reload=reload, connection=connection
        ),
    }
    columns = _get_columns(level=Level.Site, include_name=include_name)
    data = frames[Level.Site].select(", ".join(columns))
    for level in Level:
        if level is Level.Site:
            continue
        columns = _get_columns(level=level, include_name=include_name)
        parent: str = columns[1]
        join_key = parent.replace("Parent_", "")
        renames = {parent: join_key, "nameOfFeature": f"nameOfFeature_{level.name}"}
        data = data.join(
            frames[level].select(
                ", ".join(f'"{c}" AS "{renames.get(c, c)}"' for c in columns)
            ),
            condition=join_key,
            how="left",
        )
    if not case_sensitive:
        data = data.select(f"{', '.join(f'lower({c}) AS {c}' for c in data.columns)}")
    return data


def _deduplicate(
    data: DuckDBPyRelation, connection: DuckDBPyConnection, level: Level
) -> DuckDBPyRelation:
    deduplication = connection.read_csv(
        Path(PATH_PACKAGE, level.name.lower(), "deduplication.csv")
    )
    data = data.join(
        deduplication.select(
            f"{level.name}_INSPIRE_ID_cluster, {level.name}_INSPIRE_ID"
        ),
        condition=f"{level.name}_INSPIRE_ID",
        how="left",
    ).select(
        f"""*
        EXCLUDE(
            {level.name}_INSPIRE_ID_cluster
        ) REPLACE(
            COALESCE({level.name}_INSPIRE_ID_cluster, {level.name}_INSPIRE_ID) AS {level.name}_INSPIRE_ID
        )"""
    )
    return data
