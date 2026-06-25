from pathlib import Path
from typing import Final

import duckdb
import splink
import splink.comparison_library as cl

import iep._lcp
from iep.config import PATH_INPUT, PATH_PACKAGE
from iep.utils import clean

if __name__ == "__main__":
    connection = duckdb.connect()
    remove = ["kraftwerk", "central termoelectrica", "central termoeletrica"]
    columns: Final[set[str]] = {
        "PlantName",
        "FacilityName",
        "Address1",
        "Address2",
        "City",
        "PostalCode",
        "Longitude",
        "Latitude",
        "Refineries",
        "OtherSector",
    }
    lcp_links = connection.read_csv(PATH_INPUT / "links_lcp_part.csv")
    lcp = iep._lcp.load(connection=connection)
    identifiers = iep.identifiers.load(connection=connection)
    with_links = (
        lcp.join(
            lcp_links.select("Unique_Plant_ID, Installation_Part_INSPIRE_ID"),
            condition="Unique_Plant_ID",
            how="left",
        )
        .join(
            identifiers.select("Facility_INSPIRE_ID, Installation_Part_INSPIRE_ID"),
            condition="Installation_Part_INSPIRE_ID",
            how="left",
        )
        .select(
            f"""* REPLACE(
                {clean("PlantName", remove=["FacilityName", "Address1", "City"] + [f"'{r}'" for r in remove])} AS PlantName,
                {clean("FacilityName", remove=["Address1", "City"] + [f"'{r}'" for r in remove])} AS FacilityName,
                {clean("Address1", remove=["City"])} AS Address1,
                {", ".join(f"{clean(c)} AS {c}" for c in columns if c not in ("Address1", "PlantName", "FacilityName", "Refineries", "Longitude", "Latitude"))} 
            )
            """
        )
        .aggregate(
            f"""Unique_Plant_ID, {", ".join(f"MAX({c}) AS {c}" for c in columns | {"Facility_INSPIRE_ID"})}"""
        )
        .select(
            r"*, regexp_extract(UPPER(Unique_Plant_ID), '([A-Z]{2})\d+', 1) AS CountryCode"
        )
    )
    settings = splink.SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="Unique_Plant_ID",
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=["Facility_INSPIRE_ID"],
        blocking_rules_to_generate_predictions=[splink.block_on("CountryCode")],
        comparisons=[
            cl.JaroWinklerAtThresholds("PlantName"),
            cl.JaroWinklerAtThresholds("FacilityName"),
            cl.DistanceInKMAtThresholds(
                long_col="Longitude", lat_col="Latitude", km_thresholds=[0.1, 1.0, 5.0]
            ),
            cl.JaroWinklerAtThresholds("Address1"),
            cl.JaroWinklerAtThresholds("City"),
            # cl.JaroWinklerAtThresholds("Address2"),
            cl.JaroWinklerAtThresholds(
                "PostalCode", score_threshold_or_thresholds=[0.95]
            ),
            cl.ExactMatch("Refineries").configure(term_frequency_adjustments=True),
            cl.ExactMatch("OtherSector").configure(term_frequency_adjustments=True),
        ],
    )

    linker = splink.Linker(
        input_table_or_tables=[with_links],  # ty:ignore[invalid-argument-type]
        input_table_aliases=["lcp"],
        settings=settings,
        db_api=splink.DuckDBAPI(connection=connection),
    )
    linker.training.estimate_probability_two_random_records_match(
        deterministic_matching_rules=[splink.block_on("Facility_INSPIRE_ID")],
        recall=0.9,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1e7, seed=123)
    for rule in [
        splink.block_on("countryCode", "PostalCode"),
        splink.block_on("countryCode", "City"),
        splink.block_on("countryCode", "PlantName"),
        splink.block_on("countryCode", "Address1"),
        splink.block_on("countryCode", "Refineries"),
        splink.block_on("countryCode", "OtherSector"),
    ]:
        linker.training.estimate_parameters_using_expectation_maximisation(
            blocking_rule=rule,
        )

# %% match weights
# linker.visualisations.match_weights_chart()

# %% clustering
threshold_match_weight = -1.0
prediction = (
    linker.inference.predict(
        threshold_match_weight=threshold_match_weight
    ).as_duckdbpyrelation()
    # .filter("Facility_INSPIRE_ID_l IS NULL OR Facility_INSPIRE_ID_r IS NULL")
)
result = connection.sql(
    """WITH plant_facility_edges AS (
        SELECT
            CASE
                WHEN Facility_INSPIRE_ID_l IS NULL THEN Unique_Plant_ID_l
                ELSE Unique_Plant_ID_r
            END AS Unique_Plant_ID,
            CASE
                WHEN Facility_INSPIRE_ID_l IS NULL THEN Facility_INSPIRE_ID_r
                ELSE Facility_INSPIRE_ID_l
            END AS Facility_INSPIRE_ID,
            match_weight,
            match_probability,
        FROM prediction 
        WHERE (Facility_INSPIRE_ID_l IS NULL) <> (Facility_INSPIRE_ID_r IS NULL)
    )
    SELECT
        *
    FROM plant_facility_edges
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY Unique_Plant_ID ORDER BY match_probability DESC
    ) = 1
"""
)
result.order("Unique_Plant_ID").to_csv(
    Path(PATH_INPUT, "links_lcp_facility.csv").as_posix()
)
# connection.register(
#     "duplicate_free_dataset", with_links.filter("Facility_INSPIRE_ID NOT NULL")
# )
# clusters = linker.clustering.cluster_using_single_best_links(
#     df_predict=prediction,
#     duplicate_free_datasets=["duplicate_free_dataset"],
# )
# clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
#     df_predict=prediction, threshold_match_weight=threshold_match_weight
# ).as_duckdbpyrelation()
# clusters.select(
#     "*, BOOL_OR(Facility_INSPIRE_ID IS NULL) OVER (PARTITION BY cluster_id) AS has_null"
# ).filter("has_null").aggregate(
#     "cluster_id, COUNT(DISTINCT Facility_INSPIRE_ID) AS facilities"
# ).filter("facilities > 1")

# clusters.filter("cluster_id = 'ES0145'").select(
#     "cluster_id, Unique_Plant_ID, Facility_INSPIRE_ID"
# )

# %%
unique_id = "EE0102"
p = linker.inference.predict(
    # threshold_match_weight=-20.0,
)

filtered = (
    p.as_duckdbpyrelation()
    .filter(f"Unique_Plant_ID_l = '{unique_id}'")
    .order("match_probability DESC")
    .df()
    .to_dict("records")
)
linker.visualisations.waterfall_chart(records=filtered)
