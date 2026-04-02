from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_INPUT, VERSION
from iep.io import read_duckdb
from iep.misc import NA_VALUES


def load(
    reload: bool = False, connection: DuckDBPyConnection = duckdb.default_connection()
) -> DuckDBPyRelation:
    return read_duckdb(
        fn=Path(PATH_INPUT, VERSION, "2c_Function.xlsx"),
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
