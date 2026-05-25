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
    table_name: str = "4b_DesulphurisationInformation"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EPRTR_LCP": "INTEGER",
            "DesulphurisationInformationId": "INTEGER",
            "Installation_Part_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "month": "VARCHAR",
            "desulphurisationRate": "DOUBLE",
            "sulphurContent": "DOUBLE",
            "technicalJustification": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )
