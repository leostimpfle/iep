from pathlib import Path
from textwrap import dedent
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep.utils
from iep import utils
from iep.config import (
    NA_VALUES,
    PATH_IEP,
    THRESHOLD_RANGE,
    THRESHOLD_UNIT_ERROR,
    VERSION,
)
from iep.utils import Cte, CteQueue, Level, read_duckdb, sanitise_units

_IDENTIFIER: Final[str] = "Facility_INSPIRE_ID"
_POLLUTANT_RELEASE: Final[str] = "totalPollutantQuantityKg"


def _load_raw(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "2f_PollutantRelease"
    return read_duckdb(
        fn=Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EPRTR_LCP": "INTEGER",
            "PollutantReleaseId": "INTEGER",
            "Facility_INSPIRE_ID": "VARCHAR",
            "reportingYear": "INTEGER",
            "pollutantCode": "VARCHAR",
            "pollutantName": "VARCHAR",
            "medium": "VARCHAR",
            "totalPollutantQuantityKg": "DOUBLE",
            "accidentalPollutantQuantityKG": "DOUBLE",
            "methodCode": "VARCHAR",
            "methodName": "VARCHAR",
            "furtherDetails": "VARCHAR",
            "confidentialityReasonCode": "VARCHAR",
            "confidentialityReasonName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load(
    deduplicate: bool = False,
    balance: bool = False,
    sanitise: bool = False,
    interpolate: bool = False,
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
    if deduplicate:
        data = utils.deduplicate(data=data, level=Level.Facility)
    if balance:
        data = iep.utils.balance(
            data=data,
            time="reportingYear",
            groups=["Facility_INSPIRE_ID", "pollutantCode", "medium"],
        )
    if sanitise:
        data = _sanitise(data=data)
    if interpolate:
        data = _interpolate(data=data)
    return connection.sql(data.to_sql())


def _sanitise(data: CteQueue) -> CteQueue:
    data = data.extend(
        name="_sanitise",
        query=f"SELECT * FROM {data.final} WHERE pollutantCode NOT NULL AND medium NOT NULL",
    )
    data = _sanitise_biomassco2(data=data)
    data = sanitise_units(
        data=data,
        value=_POLLUTANT_RELEASE,
        time="reportingYear",
        groups=[_IDENTIFIER, "pollutantCode", "medium"],
        threshold_delta=THRESHOLD_UNIT_ERROR,
        threshold_range=THRESHOLD_RANGE,
    )
    data = _sanitise_proxy(data=data)
    return data


def _sanitise_biomassco2(data: CteQueue, threshold: float = 0.3) -> CteQueue:
    """Check consistency of CO2 and CO2EXCLBIOMASS reporting:
        - has_biomass (CO2 > CO2EXCLBIOMASS) flips year-on-year
        - CO2 has large, immediately-reversed log-change
    Set to NULL when both the change and its reversal exceed `threshold`
    """
    input_name: str = data.final
    prefix: str = "_sanitise_biomass"
    data = data.extend(
        name=f"{prefix}_base",
        query=dedent(
            f"""SELECT
                reportingYear,
                Facility_INSPIRE_ID,
                FIRST({_POLLUTANT_RELEASE}) FILTER (pollutantCode = 'CO2') AS CO2,
                FIRST({_POLLUTANT_RELEASE}) FILTER (pollutantCode = 'CO2EXCLBIOMASS') AS CO2EXCLBIOMASS 
            FROM {input_name}
            WHERE medium = 'AIR'
            GROUP BY ALL
            HAVING CO2 IS NOT NULL OR CO2EXCLBIOMASS IS NOT NULL
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_stats",
        query=dedent(
            f"""SELECT
               *,
               COALESCE("CO2", 0) > COALESCE("CO2EXCLBIOMASS", 0) AS has_biomass,
               LAG(has_biomass) OVER w AS has_biomass_lag,
               CASE WHEN "CO2" > 0 AND LAG("CO2") OVER w > 0
                   THEN LOG10("CO2" / LAG("CO2") OVER w)
               END AS CO2_log_change 
            FROM {prefix}_base 
            WINDOW w AS (PARTITION BY Facility_INSPIRE_ID ORDER BY reportingYear)
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_errors",
        query=dedent(
            f"""SELECT
                reportingYear,
                Facility_INSPIRE_ID,
                TRUE AS is_error
            FROM {prefix}_stats
            WINDOW w AS (PARTITION BY Facility_INSPIRE_ID ORDER BY "reportingYear")
            QUALIFY
                has_biomass != has_biomass_lag
                AND ABS(CO2_log_change) > {threshold} 
                AND ABS(LEAD(CO2_log_change) OVER w) > {threshold} 
                AND SIGN(CO2_log_change) != SIGN(LEAD(CO2_log_change) OVER w)
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_{input_name}",
        query=dedent(
            f"""SELECT
               t.* REPLACE(
                   CASE
                       WHEN t.pollutantCode = 'CO2'
                           AND t.medium = 'AIR'
                           AND COALESCE({prefix}_errors.is_error, FALSE)
                       THEN NULL
                       ELSE t.{_POLLUTANT_RELEASE}
                   END AS {_POLLUTANT_RELEASE} 
               )
           FROM {input_name} t
           LEFT JOIN {prefix}_errors USING (reportingYear, Facility_INSPIRE_ID)
           """
        ),
    )
    return data


def _sanitise_proxy(data: CteQueue) -> CteQueue:
    input_name: str = data.final
    prefix: str = data.hash
    time: str = "reportingYear"
    identifier: str = _IDENTIFIER
    target: str = _POLLUTANT_RELEASE
    groups: list[str] = ["pollutantCode", "medium"]
    proxy: str = _POLLUTANT_RELEASE
    cte_target = Cte(
        name=f"{prefix}_target",
        query=dedent(
            f"""SELECT
                {time},
                {identifier},
                {", ".join(groups)},
                SUM({target}) AS target
            FROM {input_name}
            WHERE pollutantCode = 'CO2' AND medium = 'AIR'
            GROUP BY ALL
            """
        ),
    )
    cte_proxy = Cte(
        name=f"{prefix}_proxy",
        query=f"""SELECT
            {time},
            {identifier},
            {", ".join(groups)},
            SUM({proxy}) AS proxy 
        FROM {input_name} 
        WHERE medium = 'AIR' AND pollutantCode IN ('SOX', 'NOX')
        GROUP BY ALL
        """,
    )
    data = data.extend(name=cte_target.name, query=cte_target.query)
    data = data.extend(name=cte_proxy.name, query=cte_proxy.query)
    data = data.extend(
        name=f"{prefix}_ratio",
        query=dedent(
            f"""SELECT
                t.{time}, 
                t.{identifier},
                {", ".join(f"t.{g}" for g in groups)},
                {", ".join(f"p.{g} AS {g}_proxy" for g in groups)},
                CASE
                    WHEN t.target > 0.0 AND p.proxy > 0.0
                    THEN p.proxy / t.target
                END AS ratio
            FROM {cte_target.name} t
            LEFT JOIN {cte_proxy.name} p
            USING ({identifier}, {time})
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_jump_target",
        query=iep.utils.is_jump(
            table=f"{prefix}_target",
            time=time,
            identifiers=[identifier] + groups,
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
            identifiers=[identifier] + groups + [f"{g}_proxy" for g in groups],
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
                t.{time},
                t.{identifier},
                {", ".join(f"t.{g}" for g in groups)},
                MAX(r.scalar) AS scalar
            FROM {prefix}_jump_target t
            LEFT JOIN {prefix}_jump_ratio r 
            USING ({time}, {identifier}, {", ".join(groups)})
            WHERE t.error AND r.error
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_{input_name}",
        query=dedent(
            f"""SELECT
                t.* REPLACE(
                    CASE
                        WHEN e.scalar NOT NULL THEN {target} * POW(10, e.scalar)
                        ELSE {target}
                    END AS {target}
                ) 
            FROM {input_name} t
            LEFT JOIN {prefix}_scalar e
            USING ({time}, {identifier}, {", ".join(groups)})
            """
        ),
    )
    return data


def _interpolate(data: CteQueue) -> CteQueue:
    input_name: str = data.final
    prefix: str = "_interpolate"
    target: tuple[str, str] = ("CO2", "AIR")
    proxies: list[tuple[str, str]] = [("NOX", "AIR"), ("SOX", "AIR")]
    data = data.extend(
        name=f"{prefix}_target",
        query=dedent(
            f"""SELECT
                Facility_INSPIRE_ID,
                reportingYear,
                {_POLLUTANT_RELEASE} AS target
            FROM {input_name}
            WHERE pollutantCode = '{target[0]}'
                AND medium = '{target[1]}' 
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_proxy",
        query=f"""SELECT
            Facility_INSPIRE_ID,
            reportingYear,
            pollutantCode AS proxy_code,
            medium AS proxy_medium,
            {_POLLUTANT_RELEASE} AS proxy 
        FROM {input_name} 
        WHERE {" OR ".join(f"pollutantCode = '{p[0]}' AND medium = '{p[1]}'" for p in proxies)}""",
    )
    data = data.extend(
        name=f"{prefix}_ratio",
        query=dedent(
            f"""SELECT
                target.Facility_INSPIRE_ID,
                target.reportingYear,
                proxy.proxy_code,
                proxy.proxy_medium,
                proxy.proxy,
                CASE
                    WHEN target.target > 0.0
                    THEN proxy.proxy / target.target
                END AS ratio
            FROM {prefix}_target target
            LEFT JOIN {prefix}_proxy proxy
            USING (Facility_INSPIRE_ID, reportingYear)
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_bounds",
        query=dedent(
            f"""SELECT
                *,
                LAST_VALUE(ratio IGNORE NULLS) OVER (
                    w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS ratio_previous,
                LAST_VALUE(
                    CASE WHEN ratio IS NOT NULL THEN reportingYear END IGNORE NULLS
                ) OVER (
                    w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS year_previous,
                FIRST_VALUE(ratio IGNORE NULLS) OVER (
                    w ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ) AS ratio_next,
                FIRST_VALUE(
                    CASE WHEN ratio IS NOT NULL THEN reportingYear END IGNORE NULLS
                ) OVER (
                    w ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ) AS year_next 
            FROM {prefix}_ratio
            WINDOW
                w AS (
                    PARTITION BY Facility_INSPIRE_ID, proxy_code, proxy_medium
                    ORDER BY reportingYear
                )
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_interpolated",
        query=dedent(
            f"""SELECT
                Facility_INSPIRE_ID,
                reportingYear,
                proxy,
                CASE
                    WHEN ratio IS NOT NULL
                        THEN ratio
                    WHEN ratio_previous IS NOT NULL AND ratio_next is NOT NULL
                        THEN ratio_previous
                            + (ratio_next - ratio_previous)
                            * (reportingYear - year_previous)::DOUBLE
                            / (year_next - year_previous)::DOUBLE
                    ELSE NULL
                END AS ratio_interpolated,
                CASE
                    WHEN proxy IS NOT NULL
                        AND ratio_interpolated IS NOT NULL
                        AND ratio_interpolated > 0
                    THEN proxy / ratio_interpolated 
                END AS interpolated 
            FROM {prefix}_bounds 
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_averaged",
        query=dedent(
            f"""SELECT
                Facility_INSPIRE_ID,
                reportingYear,
                AVG(interpolated) AS imputed
            FROM {prefix}_interpolated
            GROUP BY ALL"""
        ),
    )
    data = data.extend(
        name=f"{prefix}_{input_name}",
        query=dedent(
            f"""SELECT
                t.* REPLACE(
                    CASE
                        WHEN t.pollutantCode = '{target[0]}' AND t.medium = '{target[1]}'
                        THEN COALESCE(t.{_POLLUTANT_RELEASE}, {prefix}_averaged.imputed)
                        ELSE t.{_POLLUTANT_RELEASE}
                    END AS {_POLLUTANT_RELEASE}
                ) 
            FROM {input_name} t 
            LEFT JOIN {prefix}_averaged USING (Facility_INSPIRE_ID, reportingYear) 
            """
        ),
    )
    return data
