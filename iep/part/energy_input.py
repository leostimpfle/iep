import pathlib
from curses.ascii import DC1
from sre_compile import AT_END
from textwrap import dedent
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep
from iep.config import NA_VALUES, PATH_IEP, VERSION
from iep.utils import CteQueue, read_duckdb

_ENERGY_INPUT: Final[str] = "energyInputTJ"
_ID: Final[str] = "Installation_Part_INSPIRE_ID"


def _load_raw(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "4d_EnergyInput"
    return read_duckdb(
        fn=pathlib.Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EPRTR_LCP": "INTEGER",
            "EnergyInputId": "INTEGER",
            "Installation_Part_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "fuelInputCode": "VARCHAR",
            "fuelInputName": "VARCHAR",
            "otherSolidFuelCode": "VARCHAR",
            "otherSolidFuelName": "VARCHAR",
            "otherGaseousFuelCode": "VARCHAR",
            "otherGaseousFuelName": "VARCHAR",
            "furtherDetails": "VARCHAR",
            "energyInputTJ": "DOUBLE",
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
            groups=[
                "Installation_Part_Inspire_ID",
                "fuelInputCode",
                "otherSolidFuelCode",
                "otherGaseousFuelCode",
            ],
        )
    if sanitise:
        data = _sanitise(data=data)

    return connection.sql(data.to_sql())


def _sanitise(data: CteQueue) -> CteQueue:
    data = iep.utils.sanitise_units(
        data=data,
        value=_ENERGY_INPUT,
        time="reportingYear",
        groups=[
            "Installation_Part_Inspire_ID",
            "fuelInputCode",
            "otherSolidFuelCode",
            "otherGaseousFuelCode",
        ],
    )
    data = _sanitise_proxy(data=data)
    return data


def _sanitise_proxy(data: CteQueue) -> CteQueue:
    input_name: str = data.final
    prefix: str = "_sanitise_proxy"
    data = data.extend(
        name=f"{prefix}_emissions",
        query=iep.part.emissions.load(balance=True, sanitise=True).sql_query(),
    )
    data = data.extend(
        name=f"{prefix}_proxy",
        query=f"""SELECT
            reportingYear,
            {_ID},
            pollutantCode AS proxy_code,
            SUM(totalPollutantQuantityTNE) AS proxy 
        FROM {prefix}_emissions 
        GROUP BY ALL
        """,
    )
    data = data.extend(
        name=f"{prefix}_target",
        query=dedent(
            f"""SELECT
                reportingYear,
                {_ID},
                SUM({_ENERGY_INPUT}) AS target
            FROM {input_name}
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_ratio",
        query=dedent(
            f"""SELECT
                target.{_ID},
                target.reportingYear,
                proxy.proxy_code,
                CASE
                    WHEN target.target > 0.0
                    THEN proxy.proxy / target.target
                END AS ratio,
                LAG(ratio) OVER (
                    PARTITION BY proxy.{_ID}, proxy.proxy_code
                    ORDER BY reportingYear 
                ) AS ratio_lagged,
                CASE
                    WHEN ratio > 0.0 AND ratio_lagged > 0.0 THEN LOG10(ratio/ratio_lagged)
                END AS log_change
            FROM {prefix}_target target
            LEFT JOIN {prefix}_proxy proxy
            USING ({_ID}, reportingYear)
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_error",
        query=dedent(
            f"""SELECT
                {_ID},
                reportingYear,
                proxy_code,
                LEAD(log_change) OVER (
                    PARTITION BY {_ID}, proxy_code
                    ORDER BY reportingYear
                ) AS log_change_lead,
                ABS(log_change) > 0.5
                    AND ABS(log_change_lead) > 0.5 
                    AND SIGN(log_change) != SIGN(log_change_lead)
                AS error,
                CASE
                    WHEN ratio > 0.0 AND ratio_lagged > 0.0
                    THEN ABS(ROUND(LOG10(ratio) - LOG10(ratio_lagged)))::BIGINT
                END AS scalar
            FROM {prefix}_ratio
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_scalar",
        query=dedent(
            f"""SELECT
                reportingYear,
                {_ID},
                BOOL_OR(error) AS error,
                MAX(scalar) AS scalar
            FROM {prefix}_error
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_scalar_by_fuel",
        query=dedent(
            f"""SELECT
                t.*,
                e.error,
                LAG({_ENERGY_INPUT}) OVER w AS lagged,
                CASE
                    WHEN {_ENERGY_INPUT} > 0.0 AND lagged > 0.0
                    THEN LOG10({_ENERGY_INPUT}) - LOG10(lagged)
                END AS log_change,
                CASE
                    WHEN ABS(log_change) > 0.5 AND e.error THEN e.scalar
                END AS scalar_fuel
            FROM {input_name} t 
            LEFT JOIN {prefix}_scalar e
            USING ({_ID}, reportingYear) 
            WINDOW w AS (
                PARTITION BY {_ID}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
                ORDER BY reportingYear
            )
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_{input_name}",
        query=dedent(
            f"""SELECT
                * EXCLUDE(
                    lagged, log_change, scalar_fuel
                ) REPLACE(
                    CASE
                        WHEN scalar_fuel NOT NULL THEN {_ENERGY_INPUT} / POW(10, scalar_fuel)
                        ELSE {_ENERGY_INPUT}
                    END AS {_ENERGY_INPUT}
                )
            FROM {prefix}_scalar_by_fuel
            """
        ),
    )
    return data
