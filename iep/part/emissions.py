import math
import pathlib

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep
from iep.config import NA_VALUES, PATH_IEP, VERSION
from iep.utils import CteQueue, read_duckdb


def _load_raw(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "4e_EmissionsToAir"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EPRTR_LCP": "INTEGER",
            "EmissionsToAirId": "INTEGER",
            "Installation_Part_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "pollutantCode": "VARCHAR",
            "pollutantName": "VARCHAR",
            "totalPollutantQuantityTNE": "DOUBLE",
            "confidentialityReasonCode": "VARCHAR",
            "confidentialityReasonName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar="true",
        reload=reload,
        connection=connection,
    )


def load(
    balance: bool = False,
    sanitise: bool = False,
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = CteQueue()
    data = data.extend(
        name="_raw",
        query=_load_raw(
            version=version, reload=reload, connection=connection
        ).sql_query(),
    )
    if balance:
        data = iep.utils.balance(
            data=data,
            time="reportingYear",
            groups=["Installation_Part_Inspire_ID", "pollutantCode"],
        )
    if sanitise:
        data = _sanitise(data=data)

    return connection.sql(data.to_sql())


def _sanitise(data: CteQueue) -> CteQueue:
    data = iep.utils.sanitise_units(
        data=data,
        value="totalPollutantQuantityTNE",
        time="reportingYear",
        groups=["Installation_Part_Inspire_ID", "pollutantCode"],
        threshold=math.log10(900),  # almost a factor of 1_000 (t - kg - g)
    )
    return data
