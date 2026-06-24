from typing import Final

import duckdb
import splink
import splink.comparison_library as cl

import iep._lcp
from iep.config import PATH_INPUT
from iep.utils import clean

if __name__ == "__main__":
    connection = duckdb.connect()
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
    lcp_links = connection.read_csv(PATH_INPUT / "links_lcp.csv")
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
                {", ".join(f"{clean(c)} AS {c}" for c in columns if c not in ("Refineries", "Longitude", "Latitude"))} 
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
threshold_match_weight = 2.0
prediction = (
    linker.inference.predict(threshold_match_weight=threshold_match_weight)
    .as_duckdbpyrelation()
    .filter("Facility_INSPIRE_ID_l IS NULL OR Facility_INSPIRE_ID_r IS NULL")
)
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
unique_id = "DE4108"
p = linker.inference.predict(threshold_match_weight=-10.0)
filtered = (
    p.as_duckdbpyrelation()
    .filter(f"Unique_Plant_ID_r = '{unique_id}'")
    .order("match_probability DESC")
    .df()
    .to_dict("records")
)
linker.visualisations.waterfall_chart(records=filtered)
