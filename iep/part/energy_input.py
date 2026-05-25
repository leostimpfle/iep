import pathlib
from textwrap import dedent
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep.utils
from iep.config import (
    NA_VALUES,
    PATH_IEP,
    THRESHOLD_RANGE,
    THRESHOLD_UNIT_ERROR,
    VERSION,
)
from iep.utils import CteQueue, read_duckdb

_ENERGY_INPUT: Final[str] = "energyInputTJ"
_ID: Final[str] = "Installation_Part_INSPIRE_ID"
_GROUPS: Final[list[str]] = [
    "fuelInputCode",
    "otherSolidFuelCode",
    "otherGaseousFuelCode",
]


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


def _sanitise(data: CteQueue, deduplicate: bool = False) -> CteQueue:
    if deduplicate:
        data = data.extend(
            name=f"{data.hash}_deduplicated",
            query=dedent(
                f"""SELECT DISTINCT ON (reportingYear, {_ID}, {", ".join(_GROUPS)})
                FROM {data.final}
                ORDER BY reportingYear, {_ID}, {", ".join(_GROUPS)}
                """
            ),
        )
    data = iep.utils.sanitise_units(
        data=data,
        value=_ENERGY_INPUT,
        time="reportingYear",
        groups=[_ID] + _GROUPS,
        threshold_delta=THRESHOLD_UNIT_ERROR,
        threshold_range=THRESHOLD_RANGE,
    )
    data = _sanitise_proxy(data=data)
    return data


def _sanitise_proxy(data: CteQueue) -> CteQueue:
    input_name: str = data.final
    prefix: str = data.hash
    time: str = "reportingYear"
    identifier: str = _ID
    target: str = _ENERGY_INPUT
    groups: list[str] = ["pollutantCode"]
    proxy: str = "totalPollutantQuantityTNE"
    data = data.extend(
        name=f"{prefix}_emissions",
        query=iep.part.emissions.load(balance=True, sanitise=True).sql_query(),
    )
    data = data.extend(
        name=f"{prefix}_proxy",
        query=f"""SELECT
            {time},
            {identifier},
            HASH({", ".join(groups)}) AS proxy_id,
            SUM({proxy}) AS proxy 
        FROM {prefix}_emissions 
        GROUP BY ALL
        """,
    )
    data = data.extend(
        name=f"{prefix}_target",
        query=dedent(
            f"""SELECT
                {time},
                {identifier},
                SUM({target}) AS target
            FROM {input_name}
            WHERE {target} > 0.0
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_ratio",
        query=dedent(
            f"""SELECT
                target.{time},  
                target.{identifier},
                proxy.proxy_id,
                CASE
                    WHEN target.target > 0.0
                    THEN proxy.proxy / target.target
                END AS ratio
            FROM {prefix}_target target
            LEFT JOIN {prefix}_proxy proxy
            USING ({identifier}, {time})
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_jump_target",
        query=iep.utils.is_jump(
            table=f"{prefix}_target",
            time=time,
            identifiers=[identifier],
            value="target",
            threshold_delta=0.75,
            threshold_range=THRESHOLD_RANGE,
        ),
    )
    data = data.extend(
        name=f"{prefix}_jump_ratio",
        query=iep.utils.is_jump(
            table=f"{prefix}_ratio",
            time=time,
            identifiers=[identifier, "proxy_id"],
            value="ratio",
            threshold_delta=0.75,
            threshold_range=THRESHOLD_RANGE,
        ),
    )
    # Check if ratio for any pollutant jumps
    data = data.extend(
        name=f"{prefix}_scalar",
        query=dedent(
            f"""SELECT
                {time},
                {identifier},
                MAX(r.scalar) AS scalar
            FROM {prefix}_jump_target t
            LEFT JOIN {prefix}_jump_ratio r 
            USING ({time}, {identifier})
            WHERE t.error AND r.error
            GROUP BY ALL
            """
        ),
    )
    # Get scalar by fuel type (only adjust if energy inputs jump for a given fuel type)
    data = data.extend(
        name=f"{prefix}_scalar_by_fuel",
        query=dedent(
            f"""SELECT
                t.*,
                LAG({target}) OVER w AS lagged,
                CASE
                    WHEN {target} > 0.0 AND lagged > 0.0
                    THEN LOG10({target}) - LOG10(lagged)
                END AS log_change,
                CASE
                    WHEN e.scalar NOT NULL AND ABS(log_change) > 0.5 THEN e.scalar
                END AS scalar_fuel
            FROM {input_name} t 
            LEFT JOIN {prefix}_scalar e
            USING ({identifier}, {time}) 
            WINDOW w AS (
                PARTITION BY {identifier}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
                ORDER BY {time} 
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
                        WHEN scalar_fuel NOT NULL THEN {target}  * POW(10, scalar_fuel)
                        ELSE {target}
                    END AS {target}
                )
            FROM {prefix}_scalar_by_fuel
            """
        ),
    )
    return data
