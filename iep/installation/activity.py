import pathlib

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import NA_VALUES, PATH_IEP, VERSION
from iep.utils import read_duckdb


def load(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "3b_IEDAnnexIOtherActivity"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "IEDAnnexIActivityId": "INTEGER",
            "Installation_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "otherActivityCode": "VARCHAR",
            "otherActivityName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
