from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import NA_VALUES, PATH_IEP, VERSION
from iep.utils import read_duckdb


def load(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "2c_Function"
    return read_duckdb(
        fn=Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "FunctionId": "INTEGER",
            "Facility_INSPIRE_ID": "VARCHAR",
            "NACEMainEconomicActivityCode": "VARCHAR",
            "NACEMainEconomicActivityName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
