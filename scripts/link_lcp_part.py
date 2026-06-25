from pathlib import Path
from textwrap import dedent

import duckdb
import splink
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_INPUT, PATH_PACKAGE
from iep.utils import clean


def _load_iep(connection: DuckDBPyConnection) -> DuckDBPyRelation:
    import iep
    import iep._eprtr

    identifiers = iep.identifiers.load(connection=connection)
    facility = iep.facility.facility._load_raw(connection=connection)
    parts = iep.part.part._load_raw(connection=connection)
    energy_input = iep.part.energy_input._load_raw(connection=connection)
    eprtr = iep._eprtr.load_facility(connection=connection)
    links_to_eprtr = connection.read_csv(PATH_PACKAGE / "facility" / "links-eprtr.csv")
    facility = (
        facility.join(links_to_eprtr, condition="Facility_INSPIRE_ID", how="left")
        .join(
            eprtr.select("FacilityID, NationalID").distinct(),
            condition="FacilityID",
            how="left",
        )
        .select(
            r"""* REPLACE(
            CASE
                WHEN NationalID NOT NULL THEN NationalID
                ELSE COALESCE(
                    NULLIF(regexp_extract(Facility_INSPIRE_ID, '/(EW_EA-\d+)\.FACILITY$'), ''),
                    ProductionFacility_thematicId
                )
            END AS NationalID 
        )"""
        )
    )
    energy_input = connection.sql(
        """SELECT
            Installation_Part_INSPIRE_ID,
            reportingYear,
            SUM(energyInputTJ) AS energyInputTJ
        FROM energy_input
        GROUP BY Installation_Part_INSPIRE_ID, reportingYear 
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY Installation_Part_INSPIRE_ID
            ORDER BY reportingYear ASC 
        ) = 1
        """
    )
    data = (
        parts.select(
            f"""
            Installation_Part_INSPIRE_ID,
            {clean("nameOfFeature")} AS nameOfFeature,
            ProductionInstallationPart_thematicId,
            withinRefinery,
            YEAR(dateOfStartOfOperation) AS yearOfStartOfOperation,
            totalRatedThermalInput
            """
        )
        .join(
            energy_input.select("Installation_Part_INSPIRE_ID, energyInputTJ"),
            condition="Installation_Part_INSPIRE_ID",
            how="left",
        )
        .join(
            identifiers.select("Facility_INSPIRE_ID, Installation_Part_INSPIRE_ID"),
            condition="Installation_Part_INSPIRE_ID",
            how="left",
        )
        .join(
            facility.select(
                f"""Facility_INSPIRE_ID,
                {clean("NationalID")} AS ProductionFacility_thematicId,
                pointGeometryLon,
                pointGeometryLat,
                city,
                postalCode,
                countryCode
                """
            ),
            condition="Facility_INSPIRE_ID",
            how="left",
        )
        .select("*, Installation_Part_INSPIRE_ID AS unique_id")
    )
    return data


def _load_lcp(connection: DuckDBPyConnection) -> DuckDBPyRelation:
    import iep._lcp

    basic = iep._lcp.load_basic_data(connection=connection)
    facility = iep._lcp.load_plant(connection=connection)
    details = iep._lcp.load_plant_details(connection=connection)
    energy_input = iep._lcp.load_energy_inputs(connection=connection)
    data = connection.sql(
        dedent(
            rf"""SELECT
                facility.Unique_Plant_ID AS unique_id,
                facility.Unique_Plant_ID AS ProductionInstallationPart_thematicId,
                {clean("facility.EPRTRNationalId")} AS ProductionFacility_thematicId,
                {clean("facility.PlantName")} AS nameOfFeature, 
                -- Fill geolocation within LCP (cannot change)
                MAX(facility.Longitude) OVER (PARTITION BY facility.Unique_Plant_ID) AS pointGeometryLon,
                MAX(facility.Latitude) OVER (PARTITION BY facility.Unique_Plant_ID) AS pointGeometryLat,
                facility.City AS city,
                facility.PostalCode AS postalCode,
                CASE
                    WHEN basic.MemberState = 'UK' THEN 'GB'
                    ELSE basic.MemberState
                END AS countryCode,
                details.Refineries AS withinRefinery,
                regexp_extract(details.DateOfStartOfOperation, '\d{{4}}')::INTEGER AS yearOfStartOfOperation,
                details.MWth AS totalRatedThermalInput,
                energy_input.Biomass
                    + energy_input.OtherSolidFuels
                    + energy_input.LiquidFuels
                    + energy_input.NaturalGas
                    + energy_input.OtherGases
                AS energyInputTJ
            FROM facility 
            INNER JOIN details
                ON facility.ID = details.FK_Plant_ID
            INNER JOIN basic
                ON basic.ID = facility.FK_BasicData_ID
            LEFT JOIN energy_input
                ON facility.ID = energy_input.FK_Plant_ID
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY facility.Unique_Plant_ID
                ORDER BY basic.ReferenceYear DESC
            ) = 1
            """
        )
    )
    return data


connection = duckdb.connect()
iep_data = _load_iep(connection=connection)
lcp_data = _load_lcp(connection=connection)

settings = splink.SettingsCreator(
    link_type="link_only",
    unique_id_column_name="unique_id",
    retain_intermediate_calculation_columns=True,
    blocking_rules_to_generate_predictions=[splink.block_on("countryCode")],
    comparisons=[
        cl.ExactMatch("ProductionInstallationPart_thematicId"),
        cl.JaroWinklerAtThresholds("ProductionFacility_thematicId"),
        cl.JaroWinklerAtThresholds("nameOfFeature").configure(
            term_frequency_adjustments=True
        ),
        cl.ExactMatch("withinRefinery").configure(term_frequency_adjustments=True),
        # cl.DistanceInKMAtThresholds(
        #     lat_col="pointGeometryLat",
        #     long_col="pointGeometryLon",
        #     km_thresholds=[0.1, 1.0, 5.0, 10.0, 25.0, 50.0],
        # ),
        cl.CustomComparison(
            output_column_name="location",
            comparison_levels=[
                cll.And(
                    cll.Or(
                        cll.NullLevel("pointGeometryLat"),
                        cll.NullLevel("pointGeometryLon"),
                    ),
                    cll.NullLevel("postalCode"),
                ),
                cll.DistanceInKMLevel(
                    lat_col="pointGeometryLat",
                    long_col="pointGeometryLon",
                    km_threshold=0.1,
                ).configure(label_for_charts="Exact"),
                *(
                    cll.DistanceInKMLevel(
                        lat_col="pointGeometryLat",
                        long_col="pointGeometryLon",
                        km_threshold=d,
                    )
                    for d in [1.0, 5.0, 10.0, 25.0, 50.0]
                ),
                cll.Or(cll.ExactMatchLevel("postalCode"), cll.ExactMatchLevel("city")),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="totalRatedThermalInput",
            comparison_levels=[
                cll.NullLevel("totalRatedThermalInput"),
                cll.ExactMatchLevel("totalRatedThermalInput"),
                *(
                    cll.PercentageDifferenceLevel("totalRatedThermalInput", threshold)
                    for threshold in [0.01, 0.05, 0.1, 0.25]
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="yearOfStartOfOperation",
            comparison_levels=[
                cll.NullLevel("yearOfStartOfOperation"),
                cll.ExactMatchLevel("yearOfStartOfOperation"),
                *(
                    cll.AbsoluteDifferenceLevel("yearOfStartOfOperation", threshold)
                    for threshold in [1, 2]
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.CustomComparison(
            output_column_name="energyInputTJ",
            comparison_levels=[
                cll.NullLevel("energyInputTJ"),
                *(
                    cll.PercentageDifferenceLevel("energyInputTJ", threshold)
                    for threshold in [0.05, 0.1, 0.25]
                ),
                cll.ElseLevel(),
            ],
        ),
    ],
)

linker = splink.Linker(
    input_table_or_tables=[lcp_data, iep_data],  # ty:ignore[invalid-argument-type]
    input_table_aliases=["lcp", "iep"],
    settings=settings,
    db_api=splink.DuckDBAPI(connection=connection),
)
linker.training.estimate_probability_two_random_records_match(
    deterministic_matching_rules=[
        splink.block_on("ProductionFacility_thematicId"),
        splink.block_on("ProductionInstallationPart_thematicId"),
        splink.block_on(
            "countryCode",
            "city",
            "postalCode",
            "totalRatedThermalInput",
        ),
    ],
    recall=0.6,
)
linker.training.estimate_u_using_random_sampling(max_pairs=1e7, seed=123)
for rule in [
    splink.block_on("countryCode", "postalCode"),
    splink.block_on("countryCode", "ProductionFacility_thematicId"),
    splink.block_on("countryCode", "ProductionInstallationPart_thematicId"),
    splink.block_on("countryCode", "city"),
]:
    linker.training.estimate_parameters_using_expectation_maximisation(
        blocking_rule=rule,
    )


# %%

prediction = linker.inference.predict(threshold_match_probability=0.2)
prediction.as_duckdbpyrelation().aggregate(
    """unique_id_r AS Unique_Plant_ID,
    FIRST(unique_id_l ORDER BY match_weight DESC) AS Installation_Part_INSPIRE_ID,
    FIRST(match_weight ORDER BY match_weight DESC) AS match_weight,
    FIRST(match_probability ORDER BY match_weight DESC) AS match_probability
    """
).order("Unique_Plant_ID").to_csv(Path(PATH_INPUT, "links_lcp_part.csv").as_posix())

# %%
linker.visualisations.match_weights_chart()
unique_id = "SK0135"
unique_id = "PL0015"
unique_id = "DE1009"
unique_id = "BG0018"
p = linker.inference.predict(threshold_match_probability=0.0)
filtered = (
    p.as_duckdbpyrelation()
    .filter(f"unique_id_r = '{unique_id}'")
    .order("match_probability DESC")
    .df()
    .to_dict("records")
)
linker.visualisations.waterfall_chart(records=filtered)
