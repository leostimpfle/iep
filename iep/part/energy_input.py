import pathlib
from textwrap import dedent
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep.utils
from iep.config import (
    NA_VALUES,
    PATH_IEP,
    PATH_INPUT,
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
    sanitise: bool = False,
    add_lcp: bool = True,
    estimate_emissions_co2: bool = True,
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
    if sanitise:
        data = _standardise_fuels(data=data)
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
        data = _sanitise(data=data)
    if estimate_emissions_co2:
        data = _estimate_emissions_co2(data=data)
    return connection.sql(data.to_sql())


def _standardise_fuels(data: CteQueue) -> CteQueue:
    prefix: str = data.hash
    partition_by: Final[list[str]] = ["Installation_Part_INSPIRE_ID", "fuelInputCode"]
    order_by: Final[list[str]] = ["reportingYear"]
    data = data.extend(
        name=f"{prefix}_backfill_details",
        query=dedent(
            f"""SELECT
                * REPLACE(
                    {iep.utils.fill(column="CASE WHEN energyInputTJ > 0.0 THEN furtherDetails END", partition_by=partition_by, order_by=order_by, direction="both")} AS furtherDetails,
                    {iep.utils.fill(column="CASE WHEN energyInputTJ > 0.0 THEN otherSolidFuelCode END", partition_by=partition_by, order_by=order_by, direction="both")} AS otherSolidFuelCode,
                    {iep.utils.fill(column="CASE WHEN energyInputTJ > 0.0 THEN otherGaseousFuelCode END", partition_by=partition_by, order_by=order_by, direction="both")} AS otherGaseousFuelCode
               )
            FROM {data.final}
            """,
        ),
    )

    def is_black_liquor(column: str) -> str:
        strings: list[str] = [
            "black liquor",
            "licor negro",
            "lixívia negra",
            "ablauge",
        ]
        return f"""regexp_matches({column}, '(?i)(?:{"|".join(strings)})')"""

    data = data.extend(
        name=f"{prefix}_recoded",
        query=dedent(
            f"""SELECT
                * REPLACE(
                    -- 'Other' not informative; set to NULL
                    NULLIF(otherSolidFuelCode, 'Other') AS otherSolidFuelCode,
                    CASE
                        WHEN otherGaseousFuelCode = 'Other' THEN NULL
                        WHEN otherGaseousFuelCode = 'FurnaceGas' THEN 'BlastFurnaceGas'
                        -- black liquor is biomass
                        WHEN {is_black_liquor(column="furtherDetails")} THEN 'Biomass' 
                        WHEN regexp_matches(furtherDetails, '(?i)(?:biogas)') THEN 'Biomass'
                        ELSE otherGaseousFuelCode
                    END AS otherGaseousFuelCode,
                )
            FROM {prefix}_backfill_details
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_enhanced",
        query=dedent(
            f"""SELECT
                * REPLACE(
                    CASE
                        WHEN otherSolidFuelCode NOT NULL AND otherSolidFuelCode != 'Other'
                            THEN otherSolidFuelCode
                        WHEN otherGaseousFuelCode NOT NULL AND  otherGaseousFuelCode != 'Other'
                            THEN otherGaseousFuelCode
                        ELSE fuelInputCode
                    END AS fuelInputCode 
                )
            FROM {prefix}_recoded
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
                MAX(otherSolidFuelCode) AS otherSolidFuelCode,
                MAX(otherGaseousFuelCode) AS otherGaseousFuelCode,
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
    threshold_outlier: float = 2.0
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
                target.target,
                proxy.proxy,
                CASE
                    WHEN proxy.proxy > 0.0 THEN target.target / proxy.proxy 
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
        time=time,
        identifiers=[identifier],
        groups=groups,
        reference="ratio",
        target="target",
        threshold_quantile=0.99,
        threshold_outlier=threshold_outlier,
    )
    # Nullify ratio when outlier (for interpolation)
    data = data.extend(
        name=f"{prefix}_ratio_outlier_nullified",
        query=dedent(
            f"""SELECT
                *
                REPLACE(
                    CASE
                        WHEN is_outlier THEN NULL ELSE ratio
                    END AS ratio
                )
            FROM {prefix}_ratio_outlier
            """
        ),
    )
    # Interpolate ratio
    data = iep.utils.interpolate(
        data=data,
        table=f"{prefix}_ratio_outlier_nullified",
        time=time,
        identifiers=[identifier],
        groups=groups,
        target="ratio",
    )
    # Get outlier year-identifier pairs (aggregate across pollutantCodes)
    data = data.extend(
        name=f"{prefix}_ratio_outlier_scalar",
        query=dedent(
            f"""SELECT
                {time}, 
                {identifier},
                MAX(target) AS total_actual,
                MEDIAN(ratio * proxy) AS total_inferred,
                NULLIF(ROUND(MEDIAN(ratio_to_median)), 0) AS scalar
            FROM {prefix}_ratio_outlier_nullified_interpolated
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
                t.*,
                -- We cannot directly take `outlier.scalar` because this is aggregated across all fuelInputCodes
                -- Compare to median of surrounding rows within fuelInputCode instead
                MEDIAN({target})
                    FILTER (outlier.scalar IS NULL)
                    OVER (
                        PARTITION BY {identifier}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
                    )
                AS _median_global,
                MEDIAN({target})
                    FILTER (outlier.scalar IS NULL AND {target} > 0.0)
                    OVER (
                        PARTITION BY {identifier}, fuelInputCode, otherSolidFuelCode, otherGaseousFuelCode
                        ORDER BY {time}
                        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                    )
                AS _median_local,
                COALESCE(_median_local, _median_global) AS _median,
                -- Difference of observation to local median
                CASE
                    WHEN outlier.scalar NOT NULL AND _median > 0.0 AND {target} > 0.0 
                        THEN ROUND(LOG10({target} / _median), 0)
                END AS _delta_to_median,
                -- Nonzero target: get scalar based on local difference to median
                CASE WHEN outlier.scalar NOT NULL AND ABS(_delta_to_median) >= outlier.scalar
                    THEN NULLIF(_delta_to_median, 0)
                END AS scalar_outlier, 
                -- Zero target: flag outlier if `_median` positive (and so large that scaling matters)
                CASE
                    WHEN outlier.scalar NOT NULL AND _median >= 100.0 AND {target} = 0.0
                    THEN GREATEST(total_inferred - total_actual, 0.0) 
                END AS zero_outlier_inferred
            FROM {prefix}_with_log_delta_flag t
            LEFT JOIN {prefix}_ratio_outlier_scalar outlier
                USING ({time}, {identifier})
            """
        ),
    )
    # Sanitise energyInputTJ
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
                    _median_global,
                    _median_local,
                    _delta_to_median,
                    scalar_outlier,
                    zero_outlier_inferred
                )
                REPLACE(
                    CASE
                        -- Scale to fix unit errors
                        WHEN jump.scalar NOT NULL
                            AND t.is_large_change 
                            AND {target} / POW(10, jump.scalar)
                                BETWEEN LEAST(_median_global, _median_local) * 0.1
                                AND GREATEST(_median_global, _median_local) * 2.0 
                            THEN {target} / POW(10, jump.scalar)
                        WHEN t.scalar_outlier NOT NULL 
                            AND {target} / POW(10, t.scalar_outlier)
                                BETWEEN LEAST(_median_global, _median_local) * 0.1
                                AND GREATEST(_median_global, _median_local) * 2.0 
                            THEN {target} / POW(10, t.scalar_outlier) 
                        -- Set to value inferred from ratio interpolation if zero 
                        WHEN zero_outlier_inferred NOT NULL 
                            --THEN zero_outlier_inferred 
                            THEN NULL
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


def _add_lcp(data: CteQueue) -> CteQueue:
    import iep._lcp

    input_name = data.final
    data = data.extend(name="_lcp_raw", query=iep._lcp.load().sql_query())
    data = data.extend(
        name="_lcp_mapped",
        query=dedent(
            f"""SELECT
                COALESCE(Installation_Part_INSPIRE_ID, Unique_Plant_ID) AS Installation_Part_INSPIRE_ID,
                ReferenceYear AS reportingYear,
                SUM(Biomass) AS Biomass,
                SUM(OtherSolidFuels) AS OtherSolidFuels,
                SUM(LiquidFuels) AS LiquidFuels,
                SUM(NaturalGas) AS NaturalGas, 
                SUM(OtherGases) AS OtherGases
            FROM _lcp_raw
            LEFT JOIN (
                SELECT * FROM read_csv('{PATH_INPUT / "links_lcp_part.csv"}')
            ) USING (Unique_Plant_ID)
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name="_lcp_pivoted",
        query=dedent(
            """UNPIVOT _lcp_mapped 
            ON COLUMNS(* EXCLUDE(Installation_Part_INSPIRE_ID, reportingYear))
            INTO
                NAME fuelInputCode 
                VALUE energyInputTJ
            """
        ),
    )
    data = data.extend(
        name="_lcp_combined",
        query=dedent(
            f"""SELECT * FROM {input_name}
            UNION BY NAME
            SELECT * FROM _lcp_pivoted
            """
        ),
    )
    # Map pre-2016 `OtherSolidFuels` to post 2016 codes (Coal/Lignite/Peat)
    # Only map `OtherSolidFuels` if 2016 code is unique
    data = data.extend(
        name="_lcp_map_othersolidfuels",
        query=dedent(
            """SELECT
                Installation_PART_INSPIRE_ID,
                'OtherSolidFuels' AS fuelInputCode,
                MAX(fuelInputCode) AS otherSolidFuelCode
            FROM _lcp_combined
            WHERE energyInputTJ > 0.0
              AND reportingYear IN (2016, 2018)  -- Also check 2018 because of internal IEP mapping
              AND fuelInputCode IN ('Coal', 'Lignite', 'Peat')
            GROUP BY Installation_PART_INSPIRE_ID
            HAVING COUNT(DISTINCT fuelInputCode) = 1
            """
        ),
    )
    data = data.extend(
        name="_lcp_enhanced_fuel_code",
        query=dedent(
            """SELECT
                l.* REPLACE(
                    COALESCE(r.otherSolidFuelCode, l.fuelInputCode) AS fuelInputCode
                )
            FROM _lcp_combined l
            LEFT JOIN _lcp_map_othersolidfuels r
                USING(Installation_Part_INSPIRE_ID, fuelInputCode) 
            """
        ),
    )
    return data


def _estimate_emissions_co2(data: CteQueue) -> CteQueue:
    prefix = data.hash
    input_name = data.final
    ipcc_to_iep_fuel: Final[dict[str, str]] = {
        "Crude Oil": "LiquidFuels",
        "Natural Gas Liquids": "LiquidFuels",
        "Motor Gasoline": "LiquidFuels",
        "Aviation Gasoline": "LiquidFuels",
        "Jet Gasoline": "LiquidFuels",
        "Jet Kerosene": "LiquidFuels",
        "Other Kerosene": "LiquidFuels",
        "Shale Oil": "LiquidFuels",
        "Gas/Diesel Oil": "LiquidFuels",
        "Residual Fuel Oil": "LiquidFuels",
        "Liquefied Petroleum Gases": "LPG",
        "Bitumen": "OtherSolidFuels",
        "Lubricants": "LiquidFuels",
        "Petroleum Coke": "Coke",
        "Refinery Feedstocks": "RefineryGas",
        "Blast Furnace Gas": "BlastFurnaceGas",
        "Oxygen Steel Furnace Gas": "OxygenSteel",
        "Coke Oven Gas": "CokeOvenGas",
        "Natural Gas": "NaturalGas",
        "Anthracite": "Coal",
        "Coking Coal": "Coal",
        "Other Bituminous Coal": "Coal",
        "Sub-Bituminous Coal": "Coal",
        "Lignite": "Lignite",
        "Oil Shale and Tar Sands": "Tar",
        "Brown Coal Briquettes": "Coal",
        "Coke Oven Coke and Lignite Coke": "Coke",
        "Gas Coke": "CokeOvenGas",
        "Coal Tar": "Tar",
        "Patent Fuel": "PatentFuels",
        "Peat": "Peat",
        "Wood/Wood Waste": "Biomass",
        "Sulphite lyes (Black Liquor)": "Biomass",
        "Municipal Wastes (biomass)": "Biomass",
        "Other Primary Solid Biomass": "Biomass",
        "Landfill Gas": "OtherGases",
        "Ethane": "OtherGases",
    }
    data = data.extend(
        name=f"{prefix}_ipcc_emission_factors",
        # https://www.ipcc-nggip.iges.or.jp/public/2006gl/
        query=dedent(
            f"""SELECT
                *
            FROM read_csv('{PATH_INPUT / "ipcc2006_table_2_2.csv"}')
            """
        ),
    )
    data = data.extend(
        name="_map_ipcc_to_iep_fuel(ipcc_fuel, fuelInputCode)",
        query=dedent(
            f"""VALUES {", ".join(f"('{k}', '{v}')" for k, v in ipcc_to_iep_fuel.items())}"""
        ),
    )
    data = data.extend(
        name="ef_factors",
        query=dedent(
            f"""SELECT
                    fm.fuelInputCode,
                    AVG(ef.CO2_Default) / 1000.0 AS emission_factor_tCO2TJ
                FROM {prefix}_ipcc_emission_factors ef
                JOIN _map_ipcc_to_iep_fuel fm ON ef.Fuel = fm.ipcc_fuel
                GROUP BY fm.fuelInputCode
            """,
        ),
    )
    data = data.extend(
        name=f"{input_name}_with_emission_estimates",
        query=dedent(
            f"""SELECT
                {input_name}.*,
                emission_factor_tCO2TJ,
                energyInputTJ * emission_factor_tCO2TJ AS tCO2
            FROM {input_name}
            LEFT JOIN ef_factors USING (fuelInputCode)""",
        ),
    )
    return data
