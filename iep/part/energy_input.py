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
    data = _standardise_fuels(data=data)
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


def _standardise_fuels(data: CteQueue) -> CteQueue:
    data = data.extend(
        name=f"{data.hash}_fuel_code",
        query=dedent(
            f"""SELECT
                * REPLACE(
                    -- 'Other' not informative; set to NULL
                    NULLIF(otherSolidFuelCode, 'Other') AS otherSolidFuelCode,
                    NULLIF(otherSolidFuelName, 'Other') AS otherSolidFuelName,
                    NULLIF(otherGaseousFuelCode, 'Other') AS otherGaseousFuelCode,
                    NULLIF(otherGaseousFuelName, 'Other') AS otherGaseousFuelName
                )
            FROM {data.final}
            """
        ),
    )
    data = data.extend(
        name=f"{data.hash}_fuel_code_agg",
        query=dedent(
            f"""SELECT
                Installation_Part_INSPIRE_ID,
                reportingYear,
                fuelInputCode,
                fuelInputName,
                otherSolidFuelCode,
                otherSolidFuelName,
                otherGaseousFuelCode,
                otherGaseousFuelName,
                SUM(energyInputTJ) AS energyInputTJ
            FROM {data.final}
            GROUP BY ALL
            """
        ),
    )
    return data


def _sanitise(data: CteQueue) -> CteQueue:
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
    # Calculate log delta
    data = data.extend(
        name=f"{prefix}_with_log_delta",
        query=dedent(
            f"""SELECT
                    *,
                    LAG({target}) OVER w AS lagged,
                    CASE
                        WHEN {target} > 0.0 AND lagged > 0.0
                        THEN LOG10({target}) - LOG10(lagged)
                    END AS log_delta,  
                FROM {input_name}
                WINDOW w AS (
                    PARTITION BY {identifier}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
                    ORDER BY {time} 
                )
                """
        ),
    )
    data = data.extend(
        name=f"{prefix}_with_log_delta_flag",
        query=dedent(
            f"""SELECT
                    *,
                    ABS(log_delta) > 0.5 AS is_large_change,
                    BOOL_OR(is_large_change) OVER w AS has_large_change 
                FROM {prefix}_with_log_delta
                WINDOW w AS (
                    PARTITION BY {identifier}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
                )
                """
        ),
    )
    # Get emission proxy
    data = data.extend(
        name=f"{prefix}_emissions",
        query=iep.part.emissions.load(balance=True, sanitise=True).sql_query(),
    )
    data = data.extend(
        name=f"{prefix}_proxy",
        query=f"""SELECT
            {time},
            {identifier},
            {", ".join(groups)},
            SUM({proxy}) AS proxy 
        FROM {prefix}_emissions 
        GROUP BY ALL
        """,
    )
    # Get target: total energyInputTJ
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
    # Calculate ratio: emissions by pollutant / total energy input
    data = data.extend(
        name=f"{prefix}_ratio",
        query=dedent(
            f"""SELECT
                target.{time},  
                target.{identifier},
                {", ".join(f"proxy.{g}" for g in groups)},
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
    # Check if ratio jumps up and down driven by jump in target (rather than proxy)
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
            identifiers=[identifier] + groups,
            value="ratio",
            threshold_delta=0.75,
            threshold_range=THRESHOLD_RANGE,
        ),
    )
    # Check if ratio for any pollutant jumps
    data = data.extend(
        name=f"{prefix}_ratio_jump_scalar",
        query=dedent(
            f"""SELECT
                {time},
                {identifier},
                MAX(r.scalar) AS scalar
            FROM {prefix}_jump_target t
            LEFT JOIN {prefix}_jump_ratio r 
            USING ({time}, {identifier})
            WHERE t.is_jump AND r.is_jump
            GROUP BY ALL
            """
        ),
    )
    # Check if ratio is outlier:
    #   beyond threshold_quantile across all observations, and
    #   2x larger than median value within Installation_Part_INSPIRE_ID
    data = iep.utils.is_outlier(
        data=data,
        table=f"{prefix}_ratio",
        identifiers=[identifier],
        groups=groups,
        value="ratio",
        threshold_quantile=0.99,
        threshold_outlier=2.0,
    )
    # Get outlier year-identifier pairs (aggregate across pollutantCodes)
    data = data.extend(
        name=f"{prefix}_ratio_outlier_scalar",
        query=dedent(
            f"""SELECT
                {time}, 
                {identifier},
                MAX(scalar) AS scalar
            FROM {prefix}_ratio_outlier
            WHERE is_outlier 
            GROUP BY ALL
            """
        ),
    )
    # Get scalars by fuelInputCode:
    #   an outlier in the ratio must come from misreporting of at least one fuelInputCode
    #   identify misreported fuelInputCode by log-delta to median in non-outlier years
    data = data.extend(
        name=f"{prefix}_with_outlier_scalar",
        query=dedent(
            f"""SELECT
                *,
                MEDIAN({target}) FILTER (outlier.scalar IS NULL)
                    OVER (PARTITION BY {identifier}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode)
                AS _median,
                CASE
                    WHEN outlier.scalar NOT NULL AND {target} > 0.0 AND _median > 0.0
                    THEN ROUND(LOG10(_median / {target}), 0)
                END AS _delta_to_median,
                CASE
                    WHEN outlier.scalar NOT NULL AND _delta_to_median >= outlier.scalar 
                    THEN NULLIF(_delta_to_median, 0)
                END AS scalar_outlier, 
            FROM {prefix}_with_log_delta_flag
            LEFT JOIN {prefix}_ratio_outlier_scalar outlier
                USING ({time}, {identifier})
            """
        ),
    )
    # Scale energyInputTJ if jump or outlier
    data = data.extend(
        name=f"{prefix}_{input_name}",
        query=dedent(
            f"""SELECT
                t.*
                EXCLUDE(
                    lagged,
                    log_delta,
                    is_large_change,
                    has_large_change,
                    _median,
                    _delta_to_median,
                    scalar_outlier 
                )
                REPLACE(
                    CASE
                        WHEN jump.scalar NOT NULL AND t.is_large_change 
                            THEN {target} * POW(10, jump.scalar)
                        WHEN t.scalar_outlier NOT NULL 
                            THEN {target} * POW(10, t.scalar_outlier) 
                        ELSE {target}
                    END AS {target}
                ) 
            FROM {prefix}_with_outlier_scalar t 
            LEFT JOIN {prefix}_ratio_jump_scalar jump
                USING ({identifier}, {time})
            """
        ),
    )
    return data


if __name__ == "__main__":
    raw = _load_raw()
    sanitised = load(balance=True, sanitise=True)

    # %%
    import altair

    part = "AT.CAED/9008390317877.PART"
    part = "CZ.CHMI.0047/CZ0047.PART"
    part = "ES.CAED/002112001.PART"
    altair.Chart(
        # e.aggregate("reportingYear, fuelInputCode, SUM(energyInputTJ) AS energyInputTJ")
        raw.filter(
            f"Installation_Part_INSPIRE_ID = '{part}'",
        ).aggregate(
            "Installation_Part_INSPIRE_ID, reportingYear, fuelInputCode, SUM(energyInputTJ) AS energyInputTJ"
        )
    ).mark_area().encode(
        x="reportingYear:O", y="energyInputTJ:Q", color="fuelInputCode:N"
    ).properties(width=800, height=800).save(
        r"/Users/leonardstimpfle/Downloads/fuelinput_raw.html"
    )
    altair.Chart(
        # e.aggregate("reportingYear, fuelInputCode, SUM(energyInputTJ) AS energyInputTJ")
        sanitised.filter(
            f"Installation_Part_INSPIRE_ID = '{part}'",
        ).aggregate(
            "Installation_Part_INSPIRE_ID, reportingYear, fuelInputCode, SUM(energyInputTJ) AS energyInputTJ"
        )
    ).mark_area().encode(
        x="reportingYear:O", y="energyInputTJ:Q", color="fuelInputCode:N"
    ).properties(width=800, height=800).save(
        r"/Users/leonardstimpfle/Downloads/fuelinput_sanitised.html"
    )
