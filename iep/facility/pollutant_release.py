from dataclasses import dataclass
from pathlib import Path

import duckdb
from _duckdb import DuckDBPyConnection
from duckdb import DuckDBPyRelation

from iep.config import PATH_INPUT, PATH_PACKAGE, VERSION
from iep.io import read_duckdb
from iep.misc import NA_VALUES, Layout
from iep.tables import Cte, CteChain, balance


@dataclass(kw_only=True, frozen=True, slots=True)
class PollutantRelease:
    pollutant: str
    medium: str


def load(
    layout: Layout = "wide",
    pollutants: list[PollutantRelease] | None = None,
    sanitise: bool = True,
    balance_panel: bool = False,
    deduplicate: bool = True,
    case_sensitive_id: bool = False,
    add_national_prtrs: bool = True,
    interpolate: bool = False,
    interpolate_target: PollutantRelease = PollutantRelease(
        pollutant="CO2", medium="AIR"
    ),
    interpolate_proxies: list[PollutantRelease] | None = None,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = read_duckdb(
        fn=Path(PATH_INPUT, VERSION, "2f_PollutantRelease.xlsx"),
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
    ctes = _process_pollutant_release(
        layout=layout,
        pollutants=pollutants,
        sanitise=sanitise,
        balance_panel=balance_panel,
        deduplicate=deduplicate,
        case_sensitive_id=case_sensitive_id,
        add_national_prtrs=add_national_prtrs,
        interpolate=interpolate,
        interpolate_target=interpolate_target,
        interpolate_proxies=interpolate_proxies,
        reload=reload,
    )
    ctes = CteChain(ctes=(Cte(name="_raw", query=data.sql_query()),)).extend(ctes)
    return connection.sql(ctes.to_sql())


def _process_pollutant_release(
    layout: Layout = "wide",
    pollutants: list[PollutantRelease] | None = None,
    sanitise: bool = True,
    balance_panel: bool = False,
    deduplicate: bool = True,
    case_sensitive_id: bool = False,
    add_national_prtrs: bool = True,
    interpolate: bool = False,
    interpolate_target: PollutantRelease = PollutantRelease(
        pollutant="CO2", medium="AIR"
    ),
    interpolate_proxies: list[PollutantRelease] | None = None,
    reload: bool = False,
) -> CteChain:
    ctes = CteChain(
        ctes=(
            Cte(
                name="pollutant_quantity_per_tonne",
                query=f"SELECT *, totalPollutantQuantityKg/1e3 AS totalPollutantQuantityT FROM _raw",
            ),
        )
    )
    if not case_sensitive_id:
        ctes = ctes.extend(
            Cte(
                name=f"{ctes.final}_lowercase",
                query=f"SELECT * REPLACE (lower(Facility_INSPIRE_ID) AS Facility_INSPIRE_ID) FROM {ctes.final}",
            )
        )
    # if add_national_prtrs:
    #     ctes = _add_national_prtrs(
    #         ctes=ctes, case_sensitive_id=case_sensitive_id, reload=reload
    #     )
    if deduplicate:
        ctes = ctes.extend(
            _deduplicate(alias=ctes.final, case_sensitive_id=case_sensitive_id)
        )

    if pollutants is not None:
        conditions = " OR ".join(
            f"(pollutantCode = '{pollutant.pollutant}' AND medium = '{pollutant.medium}')"
            for pollutant in pollutants
        )
        ctes = ctes.extend(
            Cte(
                name="filter_pollutants",
                query=f"SELECT * FROM {ctes.final} WHERE {conditions}",
            )
        )
    if balance_panel:
        ctes = ctes.extend(
            balance(
                alias=ctes.final,
                time="reportingYear",
                groups=["Facility_INSPIRE_ID", "pollutantCode", "medium"],
                filter="totalPollutantQuantityKg > 0.0",
            )
        )
    if sanitise:
        ctes = ctes.extend(
            Cte(
                name="drop_missing_codes",
                query=f"SELECT * FROM {ctes.final} WHERE pollutantCode IS NOT NULL AND medium is NOT NULL",
            ),
        )
        ctes = ctes.extend(_identify_co2_biomass_errors(ctes.final))
        ctes = ctes.extend(_fix_unit_errors(ctes.final))
    if interpolate:
        ctes = ctes.extend(
            _interpolate(
                alias=ctes.final,
                target=interpolate_target,
                proxies=interpolate_proxies,
            )
        )
    if layout == "wide" and pollutants is not None:
        ctes = ctes.extend(
            Cte(
                name="to_wide",
                query=f"""SELECT
                    reportingYear,
                    Facility_INSPIRE_ID,
                    {", ".join(f"SUM(CASE WHEN pollutantCode = '{p.pollutant}' AND medium = '{p.medium}' THEN totalPollutantQuantityT END) AS t{p.pollutant}{'' if p.medium == 'AIR' else p.medium}" for p in pollutants)}
                FROM {ctes.final}
                GROUP BY ALL""",
            )
        )
    elif layout == "wide":
        ctes = ctes.extend(
            Cte(
                name="wide",
                query=f"""PIVOT (
                    SELECT
                        reportingYear,
                        Facility_INSPIRE_ID,
                        totalPollutantQuantityT,
                        't' || pollutantCode || CASE WHEN medium != 'AIR' THEN '_' || medium ELSE '' END AS _col
                    FROM {ctes.final}
                )
                ON _col
                USING FIRST(totalPollutantQuantityT)
                GROUP BY reportingYear, Facility_INSPIRE_ID""",
            ),
        )
    return ctes


# def _add_national_prtrs(
#     ctes: CteChain, case_sensitive_id: bool = False, reload: bool = False
# ) -> CteChain:
#     ctes = _add_ukprtr(ctes=ctes, case_sensitive_id=case_sensitive_id, reload=reload)
#     ctes = _add_frprtr(ctes=ctes, case_sensitive_id=case_sensitive_id, reload=reload)
#     return ctes
#
#
# def _add_ukprtr(
#     ctes: CteChain, case_sensitive_id: bool = False, reload: bool = False
# ) -> CteChain:
#     import euets.data.ukprtr.facility  # lazy import to avoid circular dependency
#
#     mapping_path = Path(euets.path_package, "data", "mapping", "iep-ukprtr.csv")
#     mapping = duckdb.sql(
#         f"""SELECT
#             lower(NationalID) AS NationalID,
#             Facility_INSPIRE_ID
#         FROM read_csv('{mapping_path}')"""
#     )
#     ukprtr = (
#         euets.data.ukprtr.facility.load(reload=reload)
#         .select('* REPLACE (lower("NationalID") AS "NationalID")')
#         .join(mapping, condition="NationalID")
#         .select(
#             f"""{'"Facility_INSPIRE_ID"' if case_sensitive_id else 'lower("Facility_INSPIRE_ID")'} AS "Facility_INSPIRE_ID",
#             "ReportingYear" AS reportingYear,
#             "PollutantRelease_PollutantCode" AS pollutantCode,
#             "PollutantRelease_MediumCode" AS medium,
#             "PollutantRelease_MethodBasisCode" AS methodCode,
#             "PollutantRelease_TotalQuantity" AS totalPollutantQuantityKg,
#             "PollutantRelease_TotalQuantity" / 1e3 AS totalPollutantQuantityT,
#             "PollutantRelease_AccidentalQuantity" AS accidentalPollutantQuantityKG"""
#         )
#     )
#     return ctes.extend(
#         other=CteChain(
#             ctes=(
#                 Cte(
#                     name=Tables.ukprtr,
#                     query=ukprtr.sql_query(),
#                 ),
#                 Cte(
#                     name="ukprtr_prepared",
#                     query=f"""SELECT * FROM {Tables.ukprtr}
#                     WHERE reportingYear IN (SELECT DISTINCT reportingYear FROM {ctes.final})""",
#                 ),
#                 Cte(
#                     name="with_ukprtr",
#                     query=f"""SELECT * FROM {ctes.final}
#                     WHERE "Facility_INSPIRE_ID" NOT IN (
#                         SELECT DISTINCT "Facility_INSPIRE_ID" FROM ukprtr_prepared
#                     )
#                     UNION ALL BY NAME
#                     SELECT * FROM ukprtr_prepared""",
#                 ),
#             )
#         )
#     )
#
#
# def _add_frprtr(
#     ctes: CteChain, case_sensitive_id: bool = False, reload: bool = False
# ) -> CteChain:
#     # lazy import to avoid circular dependency
#     from euets.data.frprtr.pollutants import load as load_frprtr
#     from euets.data.iep.facility.facility import load_facility
#
#     facility = (
#         load_facility(reload=reload)
#         .select(
#             "lower(ProductionFacility_thematicId) AS identifiant, Facility_INSPIRE_ID"
#         )
#         # TODO: Check if last Facility_INSPIRE_ID should be used
#         .aggregate(
#             aggr_expr="identifiant, FIRST(Facility_INSPIRE_ID) AS Facility_INSPIRE_ID",
#             group_expr="identifiant",
#         )
#     )
#     frprtr = (
#         load_frprtr(reload=reload)
#         .select("* REPLACE (lower(identifiant) AS identifiant)")
#         .join(facility, "identifiant")
#         .select(
#             f"""{"Facility_INSPIRE_ID" if case_sensitive_id else "lower(Facility_INSPIRE_ID)"} AS Facility_INSPIRE_ID,
#             annee_emission AS reportingYear,
#             polluant AS pollutantCode,
#             milieu AS medium,
#             quantite AS totalPollutantQuantityKg,
#             quantite / 1e3 AS totalPollutantQuantityT"""
#         )
#     )
#     return ctes.extend(
#         other=CteChain(
#             ctes=(
#                 Cte(
#                     name=Tables.frprtr,
#                     query=frprtr.sql_query(),
#                 ),
#                 Cte(
#                     name="frprtr_prepared",
#                     query=f"""SELECT * FROM {Tables.frprtr}
#                     WHERE reportingYear IN (SELECT DISTINCT reportingYear FROM {ctes.final})""",
#                 ),
#                 Cte(
#                     name="with_frprtr",
#                     query=f"""SELECT * FROM {ctes.final}
#                     WHERE Facility_INSPIRE_ID NOT IN (
#                         SELECT DISTINCT Facility_INSPIRE_ID FROM frprtr_prepared
#                     )
#                     UNION ALL BY NAME
#                     SELECT * FROM frprtr_prepared""",
#                 ),
#             )
#         )
#     )


def _deduplicate(alias: str, case_sensitive_id: bool) -> CteChain:
    path = Path(PATH_PACKAGE, "facility", "deduplication.csv")
    id_expr = (
        'lower("Facility_INSPIRE_ID")'
        if not case_sensitive_id
        else '"Facility_INSPIRE_ID"'
    )
    cluster_expr = (
        'lower("Facility_INSPIRE_ID_cluster")'
        if not case_sensitive_id
        else '"Facility_INSPIRE_ID_cluster"'
    )
    expect_unique = ["reportingYear", "Facility_INSPIRE_ID", "pollutantCode", "medium"]
    return CteChain(
        ctes=(
            Cte(
                name="deduplication",
                query=f"""SELECT
                    {id_expr} AS "Facility_INSPIRE_ID",
                    {cluster_expr} AS "Facility_INSPIRE_ID_cluster"
                FROM read_csv('{path}')""",
            ),
            Cte(
                name="deduplicated",
                query=f"""SELECT
                    t.* REPLACE (
                        COALESCE(d."Facility_INSPIRE_ID_cluster", t."Facility_INSPIRE_ID") AS "Facility_INSPIRE_ID"
                    )
                FROM {alias} t
                LEFT JOIN deduplication d USING ("Facility_INSPIRE_ID")
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {", ".join(expect_unique)} ) = 1""",  # TODO: fix deduplication and throw error https://github.com/leostimpfle/euets/issues/3
            ),
        )
    )


def _fix_unit_errors(
    alias: str,
    check_codes: list[str] | None = None,
    threshold_log_change: float = 3,
    range_min_max: float = 0.5,
    pollutant_release: str = "totalPollutantQuantityT",
) -> CteChain:
    by = '"Facility_INSPIRE_ID", "pollutantCode", "medium"'
    keys = f'{by}, "reportingYear"'
    where_clause = (
        f"WHERE pollutantCode IN ({', '.join(repr(c) for c in check_codes)})"
        if check_codes
        else ""
    )
    return CteChain(
        ctes=(
            Cte(
                name="ue_base",
                query=f"""
                    SELECT {keys}, FIRST("{pollutant_release}") AS emissions
                    FROM {alias}
                    {where_clause}
                    GROUP BY {keys}""",
            ),
            Cte(
                name="ue_stats",
                query=f"""
                    SELECT *,
                        MIN(emissions) OVER w AS _min,
                        MAX(emissions) OVER w AS _max,
                        CASE WHEN emissions > 0 AND LAG(emissions) OVER w_t > 0
                            THEN LOG10(emissions / LAG(emissions) OVER w_t) END AS log_change,
                        CASE WHEN emissions > 0 AND LAG(emissions) OVER w_t > 0
                            THEN ROUND(LOG10(emissions))::BIGINT END AS "order"
                    FROM ue_base
                    WINDOW
                        w   AS (PARTITION BY {by}),
                        w_t AS (PARTITION BY {by} ORDER BY "reportingYear")""",
            ),
            Cte(
                name="ue_factors",
                query=f"""
                    SELECT {keys}, POW(10, "order" - LEAD("order") OVER w)::DOUBLE AS factor
                    FROM ue_stats
                    WINDOW w AS (PARTITION BY {by} ORDER BY "reportingYear")
                    QUALIFY
                        ABS(log_change) > log10({threshold_log_change})
                        AND ABS(LEAD(log_change) OVER w) > log10({threshold_log_change})
                        AND SIGN(log_change) != SIGN(LEAD(log_change) OVER w)
                        AND emissions / POW(10, "order" - LEAD("order") OVER w)
                            BETWEEN _min * (1 - {range_min_max})
                                AND _max * (1 + {range_min_max})""",
            ),
            Cte(
                name="ue_corrected",
                query=f"""
                    SELECT t.* REPLACE (
                        t."{pollutant_release}" / COALESCE(f.factor, 1) AS "{pollutant_release}"
                    )
                    FROM {alias} t
                    LEFT JOIN ue_factors f USING (
                        Facility_INSPIRE_ID, pollutantCode, medium, reportingYear
                    )""",
            ),
        )
    )


def _identify_co2_biomass_errors(
    alias: str,
    pollutant_release: str = "totalPollutantQuantityT",
) -> CteChain:
    return CteChain(
        ctes=(
            Cte(
                name="cb_base",
                query=f"""SELECT
                    "reportingYear",
                    "Facility_INSPIRE_ID",
                    FIRST(CASE WHEN "pollutantCode" = 'CO2' THEN "{pollutant_release}" END) AS "CO2",
                    FIRST(CASE WHEN "pollutantCode" = 'CO2EXCLBIOMASS' THEN "{pollutant_release}" END) AS "CO2EXCLBIOMASS"
                FROM {alias}
                WHERE "medium" = 'AIR'
                    AND "pollutantCode" IN ('CO2', 'CO2EXCLBIOMASS')
                GROUP BY "reportingYear", "Facility_INSPIRE_ID"
                HAVING "CO2" IS NOT NULL OR "CO2EXCLBIOMASS" IS NOT NULL""",
            ),
            Cte(
                name="cb_stats",
                query="""SELECT
                    *,
                    (COALESCE("CO2", 0) > COALESCE("CO2EXCLBIOMASS", 0))::INTEGER AS has_biomass,
                    LAG((COALESCE("CO2", 0) > COALESCE("CO2EXCLBIOMASS", 0))::INTEGER) OVER w AS has_biomass_lag,
                    CASE WHEN "CO2" > 0 AND LAG("CO2") OVER w > 0
                        THEN LOG10("CO2" / LAG("CO2") OVER w) END AS CO2_log_change
                FROM cb_base
                WINDOW w AS (PARTITION BY "Facility_INSPIRE_ID" ORDER BY "reportingYear")""",
            ),
            Cte(
                name="cb_errors",
                query="""SELECT "reportingYear", "Facility_INSPIRE_ID", TRUE AS is_error
                FROM cb_stats
                WINDOW w AS (PARTITION BY "Facility_INSPIRE_ID" ORDER BY "reportingYear")
                QUALIFY
                    ABS(has_biomass - has_biomass_lag) = 1
                    AND ABS(CO2_log_change) > 0.3
                    AND ABS(LEAD(CO2_log_change) OVER w) > 0.3
                    AND SIGN(CO2_log_change) != SIGN(LEAD(CO2_log_change) OVER w)""",
            ),
            Cte(
                name="cb_corrected",
                query=f"""SELECT t.* REPLACE (
                    CASE
                        WHEN t."pollutantCode" = 'CO2' AND t."medium" = 'AIR' AND COALESCE(e.is_error, FALSE)
                        THEN NULL
                        ELSE t."{pollutant_release}"
                    END AS "{pollutant_release}"
                )
                FROM {alias} t
                LEFT JOIN cb_errors e USING ("reportingYear", "Facility_INSPIRE_ID")""",
            ),
        )
    )


def _interpolate(
    alias: str,
    target: PollutantRelease = PollutantRelease(pollutant="CO2", medium="AIR"),
    proxies: list[PollutantRelease] | None = None,
) -> CteChain:
    """Imputes missing target values using proxy pollutants.

    For each proxy, computes ratio = proxy / target per (facility, year), linearly
    interpolates that ratio within each facility between observed anchor years
    (limit_area="inside"), then imputes the missing target as proxy / interpolated_ratio.
    When multiple proxies are given the imputed values are averaged.

    Operates on long-format data (before any PIVOT). Both target and all proxy
    pollutants must be present in `alias`; the caller is responsible for ensuring
    they are not filtered out before this step.
    """
    if proxies is None:
        proxies = [PollutantRelease(pollutant="NOX", medium="AIR")]

    proxy_conditions = " OR ".join(
        f"(pollutantCode = '{p.pollutant}' AND medium = '{p.medium}')" for p in proxies
    )
    return CteChain(
        ctes=(
            Cte(
                name="_interp_target",
                query=f"""
                    SELECT "Facility_INSPIRE_ID", reportingYear, totalPollutantQuantityT AS target_val
                    FROM {alias}
                    WHERE pollutantCode = '{target.pollutant}' AND medium = '{target.medium}'
                """,
            ),
            Cte(
                name="_interp_proxy",
                query=f"""
                    SELECT
                        "Facility_INSPIRE_ID",
                        reportingYear,
                        pollutantCode AS proxy_code,
                        medium AS proxy_medium,
                        totalPollutantQuantityT AS proxy_val
                    FROM {alias}
                    WHERE {proxy_conditions}
                """,
            ),
            Cte(
                name="_interp_ratio",
                # Start from the target (CO2) grid so the window function sees all
                # CO2 anchor years, including those beyond the proxy's observed range.
                query="""
                    SELECT
                        t."Facility_INSPIRE_ID",
                        t.reportingYear,
                        p.proxy_code,
                        p.proxy_medium,
                        p.proxy_val,
                        CASE WHEN t.target_val > 0
                            THEN p.proxy_val / t.target_val
                        END AS ratio
                    FROM _interp_target t
                    LEFT JOIN _interp_proxy p USING ("Facility_INSPIRE_ID", reportingYear)
                """,
            ),
            Cte(
                name="_interp_bounds",
                query="""
                    SELECT
                        *,
                        LAST_VALUE(ratio IGNORE NULLS) OVER (
                            PARTITION BY "Facility_INSPIRE_ID", proxy_code, proxy_medium
                            ORDER BY reportingYear
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ) AS prev_val,
                        LAST_VALUE(
                            CASE WHEN ratio IS NOT NULL THEN reportingYear END IGNORE NULLS
                        ) OVER (
                            PARTITION BY "Facility_INSPIRE_ID", proxy_code, proxy_medium
                            ORDER BY reportingYear
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ) AS prev_year,
                        FIRST_VALUE(ratio IGNORE NULLS) OVER (
                            PARTITION BY "Facility_INSPIRE_ID", proxy_code, proxy_medium
                            ORDER BY reportingYear
                            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                        ) AS next_val,
                        FIRST_VALUE(
                            CASE WHEN ratio IS NOT NULL THEN reportingYear END IGNORE NULLS
                        ) OVER (
                            PARTITION BY "Facility_INSPIRE_ID", proxy_code, proxy_medium
                            ORDER BY reportingYear
                            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                        ) AS next_year
                    FROM _interp_ratio
                """,
            ),
            Cte(
                name="_interp_ratio_filled",
                query="""
                    SELECT
                        "Facility_INSPIRE_ID",
                        reportingYear,
                        proxy_val,
                        CASE
                            WHEN ratio IS NOT NULL THEN ratio
                            WHEN prev_val IS NOT NULL AND next_val IS NOT NULL
                                THEN prev_val
                                    + (next_val - prev_val)
                                    * (reportingYear - prev_year)::DOUBLE
                                    / (next_year - prev_year)::DOUBLE
                            ELSE NULL
                        END AS interpolated_ratio
                    FROM _interp_bounds
                """,
            ),
            Cte(
                name="_interp_imputed_per_proxy",
                query="""
                    SELECT
                        "Facility_INSPIRE_ID",
                        reportingYear,
                        CASE
                            WHEN proxy_val IS NOT NULL
                                AND interpolated_ratio IS NOT NULL
                                AND interpolated_ratio > 0
                            THEN proxy_val / interpolated_ratio
                        END AS imputed
                    FROM _interp_ratio_filled
                """,
            ),
            Cte(
                name="_interp_imputed",
                query="""
                    SELECT
                        "Facility_INSPIRE_ID",
                        reportingYear,
                        AVG(imputed) AS imputed_target
                    FROM _interp_imputed_per_proxy
                    GROUP BY "Facility_INSPIRE_ID", reportingYear
                """,
            ),
            Cte(
                name="_interpolated",
                query=f"""
                    SELECT t.* REPLACE (
                        CASE
                            WHEN t.pollutantCode = '{target.pollutant}'
                                AND t.medium = '{target.medium}'
                            THEN COALESCE(t.totalPollutantQuantityT, i.imputed_target)
                            ELSE t.totalPollutantQuantityT
                        END AS totalPollutantQuantityT,
                        CASE
                            WHEN t.pollutantCode = '{target.pollutant}'
                                AND t.medium = '{target.medium}'
                            THEN COALESCE(t.totalPollutantQuantityKg, i.imputed_target * 1e3)
                            ELSE t.totalPollutantQuantityKg
                        END AS totalPollutantQuantityKg
                    )
                    FROM {alias} t
                    LEFT JOIN _interp_imputed i USING ("Facility_INSPIRE_ID", reportingYear)
                """,
            ),
        )
    )
