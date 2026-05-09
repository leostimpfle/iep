import pathlib

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_IEP
from iep.io import NA_VALUES, read_duckdb
from iep.versions import VERSION


def load(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "3j_OtherRelevantChapters"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "OtherRelevantChaptersId": "INTEGER",
            "Installation_INSPIRE_ID": "VARCHAR",
            "otherRelevantChaptersCode": "VARCHAR",
            "otherRelevantChaptersName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
