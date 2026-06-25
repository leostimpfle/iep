import pathlib
from textwrap import dedent

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep.utils
from iep.config import (
    NA_VALUES,
    PATH_IEP,
    PATH_INPUT,
    THRESHOLD_RANGE,
    THRESHOLD_UNIT_ERROR,
    VERSION,
)
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
    add_lcp: bool = True,
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
    if add_lcp:
        data = _add_lcp(data)
    if balance:
        data = iep.utils.balance(
            data=data,
            time="reportingYear",
            groups=["Installation_Part_Inspire_ID", "pollutantCode"],
        )
    if sanitise:
        data = _sanitise(data=data)

    return connection.sql(data.to_sql())


def _add_lcp(data: CteQueue) -> CteQueue:
    import iep._lcp

    input_name = data.final
    data = data.extend(name="_lcp_emissions_raw", query=iep._lcp.load().sql_query())
    data = data.extend(
        name="_lcp_emissions_mapped",
        query=dedent(
            f"""SELECT
                Installation_Part_INSPIRE_ID,
                ReferenceYear AS reportingYear,
                SUM(NOx) AS NOX, 
                SUM(SO2) AS SO2,
                SUM(Dust) AS DUST
            FROM _lcp_emissions_raw
            INNER JOIN (
                SELECT * FROM read_csv('{PATH_INPUT / "links_lcp_part.csv"}')
            ) USING (Unique_Plant_ID)
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name="_lcp_emissions_pivoted",
        query=dedent(
            """UNPIVOT _lcp_emissions_mapped 
            ON COLUMNS(* EXCLUDE(Installation_Part_INSPIRE_ID, reportingYear))
            INTO
                NAME pollutantCode
                VALUE totalPollutantQuantityTNE
            """
        ),
    )
    data = data.extend(
        name="_lcp_emissions_combined",
        query=dedent(
            f"""SELECT * FROM {input_name}
            UNION BY NAME
            SELECT * FROM _lcp_emissions_pivoted
            """
        ),
    )
    return data


def _sanitise(data: CteQueue) -> CteQueue:
    data = iep.utils.sanitise_units(
        data=data,
        value="totalPollutantQuantityTNE",
        time="reportingYear",
        groups=["Installation_Part_Inspire_ID", "pollutantCode"],
        threshold_delta=THRESHOLD_UNIT_ERROR,
        threshold_range=THRESHOLD_RANGE,
    )
    return data
