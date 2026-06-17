from pathlib import Path

import duckdb
import splink
import splink.comparison_level_library as cll
import splink.comparison_library as cl

import iep
from iep.config import PATH_PACKAGE
from iep.utils import clean
from scripts.deduplicate_facilities import (
    load_legal_entity,
    strip_legal_suffixes,
)

if __name__ == "__main__":
    connection = duckdb.connect()
    facilities = iep.facility.facility._load_raw(connection=connection)
    identifiers = iep.identifiers.load(connection=connection)
    (
        facilities.filter(
            "ProductionFacility_thematicIdScheme = 'LCPmapping_unmapped_PlantId'"
        )
        .select(
            r"""* REPLACE(
                regexp_extract(parentCompanyName, 'parentCompany of plant:\s+(.*)', 1) AS parentCompanyName 
            )
            """
        )
        .select(
            f"""* REPLACE(
                {clean("parentCompanyName", remove=["city"])} AS parentCompanyName,
                lower(streetName) AS streetName,
                lower(city) AS city,
                lower(postalCode) AS postalCode,
            )"""
        )
        .select("*, parentCompanyName AS nameOfFeature_Installation_Part")
        .select(
            "*, ROW_NUMBER() OVER (ORDER BY Facility_INSPIRE_ID, ProductionFacility_thematicId) AS unique_id"
        )
        .select("* REPLACE('lcp_' || unique_id AS unique_id)")
        .create("lcps")
    )
    (
        facilities.filter(
            """facilityType = 'EPRTR' -- Facilities of LCPs must be in E-PRTR!
            AND (
                ProductionFacility_thematicIdScheme IS NULL
                OR ProductionFacility_thematicIdScheme != 'LCPmapping_unmapped_PlantId'
            )
            """
        )
        .join(
            identifiers.select(
                "Facility_INSPIRE_ID, nameOfFeature_Installation_Part"
            ).distinct(),
            condition="Facility_INSPIRE_ID",
            how="inner",
        )
        .join(
            iep.facility.pollutant_release._load_raw(connection=connection)
            .filter("pollutantCode = 'CO2' AND medium = 'AIR'")
            .aggregate("Facility_INSPIRE_ID, SUM(totalPollutantQuantityKg) AS kgCO2"),
            condition="Facility_INSPIRE_ID",
            how="left",
        )
        .select(
            f"""* REPLACE(
                {clean("parentCompanyName", remove=["city"])} AS parentCompanyName,
                {clean("nameOfFeature", remove=["city", "parentCompanyName"])} AS nameOfFeature,
                {clean("nameOfFeature_Installation_Part", remove=["city", "parentCompanyName", "nameOfFeature"])} AS nameOfFeature_Installation_Part,
                {clean("ProductionFacility_thematicId")} AS ProductionFacility_thematicId,
                lower(streetName) AS streetName,
                lower(city) AS city,
                lower(postalCode) AS postalCode,
            )"""
        )
        .select(
            # Take first largst emitters (only relevant to tie-break `best_match` below)
            "*, ROW_NUMBER() OVER (ORDER BY kgCO2 DESC, Facility_INSPIRE_ID ASC) AS unique_id"
        )
        .select("* EXCLUDE(kgCO2) REPLACE('parent_' || unique_id AS unique_id)")
        .create("parents")
    )
    load_legal_entity(connection=connection).create("legal_entity_identifiers")
    lcps = connection.sql(
        strip_legal_suffixes(
            table="lcps",
            legal_entity_identifiers="legal_entity_identifiers",
            columns=["parentCompanyName", "nameOfFeature_Installation_Part"],
        )
    )
    parents = connection.sql(
        strip_legal_suffixes(
            table="parents",
            legal_entity_identifiers="legal_entity_identifiers",
            columns=[
                "parentCompanyName",
                "nameOfFeature",
                "nameOfFeature_Installation_Part",
            ],
        )
    )
    settings = splink.SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        additional_columns_to_retain=["Facility_INSPIRE_ID"],
        retain_intermediate_calculation_columns=True,
        blocking_rules_to_generate_predictions=[splink.block_on("countryCode")],
        comparisons=[
            cl.CustomComparison(
                output_column_name="name",
                comparison_levels=[
                    cll.And(
                        cll.NullLevel("parentCompanyName"),
                        cll.NullLevel("nameOfFeature_Installation_Part"),
                    ),
                    cll.ExactMatchLevel("nameOfFeature_Installation_Part"),
                    *(
                        cll.CustomLevel(
                            label_for_charts=f"Approximate {threshold}",
                            sql_condition=f"""contains(nameOfFeature_Installation_Part_r, parentCompanyName_l)
                            OR contains(nameOfFeature_Installation_Part_r, nameOfFeature_l) 
                            OR jaro_winkler_similarity(nameOfFeature_Installation_Part_l, nameOfFeature_Installation_Part_r) > {threshold}
                            """,
                        )
                        for threshold in [0.9, 0.7]
                    ),
                    cll.ElseLevel(),
                ],
            ),
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
                    cll.Or(
                        cll.And(
                            cll.ExactMatchLevel("postalCode"),
                            cll.JaroWinklerLevel("streetName", distance_threshold=0.9),
                            cll.JaroWinklerLevel(
                                "buildingNumber", distance_threshold=0.9
                            ),
                        ),
                        cll.DistanceInKMLevel(
                            lat_col="pointGeometryLat",
                            long_col="pointGeometryLon",
                            km_threshold=1.0,
                        ),
                    ),
                    *(
                        cll.DistanceInKMLevel(
                            lat_col="pointGeometryLat",
                            long_col="pointGeometryLon",
                            km_threshold=d,
                        )
                        for d in [10.0, 25.0, 50.0]
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.ExactMatch("mainActivityCode").configure(
                term_frequency_adjustments=True
            ),
            cl.CustomComparison(
                output_column_name="Identifier",
                comparison_levels=[
                    cll.CustomLevel(
                        sql_condition="ProductionFacility_thematicId_l IS NULL OR parentCompanyName_r IS NULL"
                    ).configure(is_null_level=True),
                    cll.CustomLevel(
                        sql_condition="contains(parentCompanyName_r, regexp_replace(ProductionFacility_thematicId_l, '^0+', ''))"
                    ).configure(
                        m_probability=0.9999,
                        fix_m_probability=True,
                        u_probability=0.0001,
                        fix_u_probability=True,
                    ),
                    cll.ElseLevel(),
                ],
            ),
        ],
    )
    linker = splink.Linker(
        input_table_or_tables=[parents, lcps],  # ty:ignore[invalid-argument-type]
        settings=settings,
        db_api=splink.DuckDBAPI(connection=connection),
    )
    linker.training.estimate_probability_two_random_records_match(
        deterministic_matching_rules=[
            splink.block_on(
                "countryCode",
                "city",
                "mainActivityCode",
            ),
            splink.block_on(
                "countryCode",
                "postalCode",
                "mainActivityCode",
            ),
            splink.block_on(
                "countryCode",
                "streetName",
                "mainActivityCode",
            ),
        ],
        recall=0.7,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1e8)
    for rule in [
        splink.block_on("countryCode", "nameOfFeature_Installation_Part"),
        splink.block_on("countryCode", "postalCode"),
        splink.block_on("countryCode", "streetName"),
        splink.block_on("countryCode", "city"),
        splink.block_on("countryCode", "mainActivityCode"),
    ]:
        linker.training.estimate_parameters_using_expectation_maximisation(
            blocking_rule=rule
        )

    prediction = linker.inference.predict(threshold_match_probability=0.5)
    # Get best match for each unmapped LCP
    best_match = prediction.as_duckdbpyrelation()
    best_match = connection.sql(
        """SELECT
            Facility_INSPIRE_ID_r AS Facility_INSPIRE_ID,
            Facility_INSPIRE_ID_l AS Facility_INSPIRE_ID_cluster,
            match_probability
        FROM best_match QUALIFY ROW_NUMBER() OVER (
            PARTITION BY Facility_INSPIRE_ID_r
            ORDER BY match_weight DESC, unique_id_l ASC
        ) = 1
        """
    )
    # Enforce one-to-one matching
    best_match = connection.sql(
        """SELECT
                Facility_INSPIRE_ID_cluster,
                Facility_INSPIRE_ID
           FROM best_match
           WHERE NOT Facility_INSPIRE_ID IN
                     (SELECT DISTINCT Facility_INSPIRE_ID_cluster FROM best_match)
        """
    ).order("Facility_INSPIRE_ID_cluster")
    best_match.to_csv(
        Path(PATH_PACKAGE, "_input", "deduplication_lcpmapping.csv").as_posix()
    )

    # %%
    linker.visualisations.m_u_parameters_chart()
    linker.visualisations.match_weights_chart()
    p = linker.inference.predict(threshold_match_probability=0.0)
    fid = "AT.CAED/9008390975237.FACILITY"
    fid = "AT.EEA/AT0084.FACILITY"
    fid = "BG.CAED/017000006.FACILITY"
    fid = "DE.EEA/DE5062.FACILITY"
    fid = "LT.CAED/166451720.FACILITY"
    fid = "AT.EEA/AT0002.FACILITY"
    fid = "AT.EEA/AT0084.FACILITY"
    fid = "FR.EEA/FR0392.FACILITY"
    fid = "FR.EEA/059.06226.FACILITY"
    filtered = (
        p.as_duckdbpyrelation()
        .filter(f"Facility_INSPIRE_ID_l = '{fid}'")
        .order("match_probability DESC")
        .limit(5)
        .df()
        .to_dict("records")
    )
    linker.visualisations.waterfall_chart(records=filtered)

    # %% check unmapped
    deduplication = duckdb.read_csv(
        Path(PATH_PACKAGE, "_input", "deduplication_lcpmapping.csv")
    )
    identifiers = iep.identifiers.load()
    facilities = iep.facility.facility._load_raw()
    energy_inputs = iep.part.energy_input.load()

    ei = energy_inputs.join(
        identifiers.select(
            "Facility_INSPIRE_ID, Installation_Part_INSPIRE_ID"
        ).distinct(),
        condition="Installation_Part_INSPIRE_ID",
        how="left",
    )

    unmapped = (
        facilities.filter(
            "ProductionFacility_thematicIdScheme = 'LCPmapping_unmapped_PlantId'"
        )
        .select("Facility_INSPIRE_ID")
        .join(
            deduplication.select("Facility_INSPIRE_ID"),
            condition="Facility_INSPIRE_ID",
            how="anti",
        )
        .join(ei, condition="Facility_INSPIRE_ID", how="inner")
    )
    unmapped.filter("energyInputTJ > 0.0").aggregate(
        "Facility_INSPIRE_ID, Installation_Part_INSPIRE_ID, SUM(energyInputTJ) AS tj"
    ).order("tj DESC")
    # %%
    fid = (
        # "https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/arb-2017-513000-500-0342658",
        # "https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/arb-2017-513000-500-0053929",
        # "DE.EEA/DE5062.FACILITY",
        "LT.CAED/166451720.FACILITY",
        "LT.EEA/LT0032.FACILITY",
        "LT.EEA/LT0033.FACILITY",
        "LT.EEA/LT0126.FACILITY",
    )
    ei.filter(f"Facility_INSPIRE_ID IN {fid}").aggregate(
        "reportingYear, Facility_INSPIRE_ID, SUM(energyInputTJ) AS tj"
    ).order("Facility_INSPIRE_ID, reportingYear")
