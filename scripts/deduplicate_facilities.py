from pathlib import Path
from textwrap import dedent
from typing import Iterable

import duckdb
import splink
import splink.blocking_rule_library as br
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep
from iep.config import PATH_PACKAGE
from iep.versions import stack_versions


def clean(column: str, remove: list[str] | None = None) -> str:
    expression = f"lower({column})"
    expression = f"trim(regexp_replace({expression}, '[^\\w\\s]', '', 'g'))"
    for r in remove or []:
        cleaned_r = f"trim(regexp_replace(lower({r}), '[^\\w\\s]', '', 'g'))"
        expression = f"replace({expression}, COALESCE({cleaned_r}, ''), '')"
    expression = f"trim({expression})"
    return f"NULLIF({expression}, '') AS {column}"


def load_legal_entity(
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    raw = connection.read_csv(
        Path(__file__).parent / "2023-09-28-elf-code-list-v1.5.csv"
    )
    return raw.query(
        "raw",
        dedent(
            """SELECT DISTINCT regexp_replace(lower(TRIM(word)), '[^a-z0-9]', '',
                                              'g') AS word
               FROM (SELECT UNNEST(string_split(val, ' ')) AS word
                     FROM (SELECT UNNEST(string_split("Entity Legal Form name Local name",
                                                      ';')) AS val
                           FROM raw
                           UNION
                           SELECT UNNEST(string_split(
                                   "Entity Legal Form name Transliterated name (per ISO 01-140-10)",
                                   ';')) AS val
                           FROM raw
                           UNION
                           SELECT UNNEST(string_split("Abbreviations Local language", ';')) AS val
                           FROM raw
                           UNION
                           SELECT UNNEST(string_split("Abbreviations transliterated", ';')) AS val
                           FROM raw))
               WHERE regexp_replace(lower(TRIM(word)), '[^a-z0-9]', '', 'g') != ''"""
        ),
    )


def strip_legal_suffixes(
    table: str, legal_entity_identifiers: str, columns: Iterable[str]
) -> str:
    ctes = ",\n".join(
        dedent(
            f"""words_{c} AS (
                SELECT
                    unique_id,
                    unnest(string_split({c}, ' ')) AS word,
                    generate_subscripts(string_split({c}, ' '), 1) AS pos
                FROM {table}
            ),
            filtered_{c} AS (
                SELECT unique_id, string_agg(word, ' ' ORDER BY pos) AS {c}
                FROM words_{c}
                WHERE regexp_replace(word, '[^a-z0-9]', '', 'gi') NOT IN (SELECT word FROM {legal_entity_identifiers})
                GROUP BY unique_id
            )"""
        )
        for c in columns
    )
    joins = "\n".join(f"LEFT JOIN filtered_{c} USING (unique_id)" for c in columns)
    return dedent(
        f"""WITH {ctes}
            SELECT
                * EXCLUDE({", ".join(columns)}),
                {", ".join(f"filtered_{c}.{c}" for c in columns)}
            FROM {table}
            {joins}"""
    )


if __name__ == "__main__":
    connection = duckdb.connect()
    facilities = stack_versions(
        loader=iep.facility.facility._load_raw, reload=False, connection=connection
    )
    # Take first non-null value by Facility_INSPIRE_ID (this reduces the risks of values changing between years for duplicates, e.g. `parentCompanyName`)
    facilities = connection.sql(
        """SELECT Facility_INSPIRE_ID,
                  ANY_VALUE(COLUMNS(* EXCLUDE Facility_INSPIRE_ID) ORDER BY reportingYear)
           FROM facilities
           GROUP BY Facility_INSPIRE_ID
        """
    )
    # Add NACE industry code
    facilities = facilities.join(
        iep.facility.function.load(connection=connection).aggregate(
            """Facility_INSPIRE_ID,
            MAX(NACEMainEconomicActivityCode) AS NACEMainEconomicActivityCode
            """
        ),
        condition="Facility_INSPIRE_ID",
        how="left",
    )
    pollutants = iep.facility.pollutant_release._load_raw(
        reload=False, connection=connection
    )
    (
        facilities.select(
            f"""* REPLACE(
                {clean("parentCompanyName", remove=["city"])},
                {clean("nameOfFeature", remove=["city", "parentCompanyName"])},
                lower(city) AS city,
                lower(postalCode) AS postalCode,
            )"""
        )
        .select(
            """* REPLACE(
                NULLIF(nameOfFeature, parentCompanyName) AS nameOfFeature
            )"""
        )
        .join(
            pollutants.aggregate(
                """Facility_INSPIRE_ID,
                MIN(reportingYear) AS year_first,
                MAX(reportingYear) AS year_last,
                FIRST(totalPollutantQuantityKg ORDER BY reportingYear) FILTER(medium = 'AIR' AND pollutantCode = 'CO2') AS CO2_first,
                LAST(totalPollutantQuantityKg ORDER BY reportingYear) FILTER(medium = 'AIR' AND pollutantCode = 'CO2') AS CO2_last
                """
            ),
            condition="Facility_INSPIRE_ID",
            how="left",
        )
        .select("*, ROW_NUMBER() OVER (ORDER BY year_first, year_last) AS unique_id")
        .create("data")
    )

    settings = splink.SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        additional_columns_to_retain=["Facility_INSPIRE_ID"],
        retain_intermediate_calculation_columns=True,
        blocking_rules_to_generate_predictions=[
            br.CustomRule(
                """l.countryCode = r.countryCode AND l.year_last < r.year_first"""
            )
        ],
        comparisons=[
            # Check parentCompanyName also against nameOfFeature
            cl.CustomComparison(
                output_column_name="parentCompanyName",
                comparison_levels=[
                    cll.NullLevel("parentCompanyName"),
                    cll.CustomLevel(
                        label_for_charts="Eaxact match",
                        sql_condition="""parentCompanyName_l = parentCompanyName_r
                        OR nameOfFeature_l = parentCompanyName_r
                        OR parentCompanyName_l = nameOfFeature_r
                        """,
                    ),
                    *(
                        cll.CustomLevel(
                            label_for_charts="Jaro-Winkler ",
                            sql_condition=f"""jaro_winkler_similarity(parentCompanyName_l, parentCompanyName_r) > {threshold}
                            OR jaro_winkler_similarity(nameOfFeature_l, parentCompanyName_r) > {threshold}
                            OR jaro_winkler_similarity(parentCompanyName_l, nameOfFeature_r) > {threshold}
                            """,
                        )
                        for threshold in [0.9, 0.7]
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.JaroWinklerAtThresholds("nameOfFeature").configure(
                term_frequency_adjustments=True
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
                        for d in [5.0, 10.0, 25.0, 50.0]
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.CustomComparison(
                output_column_name="activity",
                comparison_levels=[
                    cll.And(
                        cll.NullLevel("NACEMainEconomicActivityCode"),
                        cll.NullLevel("mainActivityCode"),
                    ).configure(is_null_level=True),
                    cll.Or(
                        cll.ExactMatchLevel("NACEMainEconomicActivityCode"),
                        cll.ExactMatchLevel("mainActivityCode"),
                    ),
                    cll.ElseLevel(),
                ],
            ).configure(term_frequency_adjustments=True),
            cl.CustomComparison(
                output_column_name="CO2",
                comparison_levels=[
                    cll.CustomLevel(
                        "(CO2_last_l IS NULL OR CO2_first_l IS NULL)"
                    ).configure(is_null_level=True),
                    *(
                        cll.CustomLevel(
                            f"ABS(TRY(LN(CO2_last_l)) - TRY(LN(CO2_first_r))) < {threshold}"
                        )
                        for threshold in (0.1, 0.25, 0.5)
                    ),
                    cll.ElseLevel(),
                ],
            ),
        ],
    )
    linker = splink.Linker(
        input_table_or_tables=connection.table("data"),  # ty:ignore[invalid-argument-type]
        settings=settings,
        db_api=splink.DuckDBAPI(connection=connection),
    )
    linker.training.estimate_probability_two_random_records_match(
        deterministic_matching_rules=[
            splink.block_on(
                "countryCode",
                "parentCompanyName",
                # "nameOfFeature",
                # "streetName",
                "city",
                "postalCode",
                "mainActivityCode",
                "NACEMainEconomicActivityCode",
            ),
        ],
        recall=0.7,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1e8)
    for rule in [
        splink.block_on("countryCode", "postalCode"),
        splink.block_on("countryCode", "parentCompanyName"),
        splink.block_on("countryCode", "nameOfFeature"),
        splink.block_on("countryCode", "city", "mainActivityCode"),
    ]:
        linker.training.estimate_parameters_using_expectation_maximisation(
            blocking_rule=rule,
        )

    prediction = linker.inference.predict(threshold_match_probability=0.90)
    # Get best match for each right-hand side facility
    best_match = prediction.as_duckdbpyrelation().aggregate(
        """Facility_INSPIRE_ID_r AS Facility_INSPIRE_ID_cluster,
        ARG_MAX(Facility_INSPIRE_ID_l, match_weight) AS Facility_INSPIRE_ID,
        ARG_MAX(match_probability, match_weight) AS match_probability 
        """
    )
    # Enforce one-to-one matching
    best_match = connection.sql(
        """SELECT Facility_INSPIRE_ID_cluster,
                  Facility_INSPIRE_ID
           FROM best_match
           WHERE NOT Facility_INSPIRE_ID IN
                     (SELECT DISTINCT Facility_INSPIRE_ID_cluster FROM best_match)
        """
    ).order("Facility_INSPIRE_ID_cluster")
    best_match.to_csv(
        Path(PATH_PACKAGE, "_input", "deduplication_facility.csv").as_posix()
    )

    # %% clustering
    # clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    #     prediction, threshold_match_probability=0.75
    # )
    # output = (
    #     clusters.as_duckdbpyrelation()
    #     .select(
    #         """*,
    #         ARG_MAX(Facility_INSPIRE_ID, year_last) OVER (PARTITION BY cluster_id) AS Facility_INSPIRE_ID_cluster,
    #         COUNT(*) OVER (PARTITION BY cluster_id) AS counts
    #         """
    #     )
    #     .filter("counts BETWEEN 1 AND 3")
    #     .select("cluster_id, Facility_INSPIRE_ID_cluster, Facility_INSPIRE_ID")
    # )
    # output.to_csv(Path(PATH_PACKAGE, "facility", "deduplication.csv").as_posix())

    # %% visualisation
    linker.visualisations.match_weights_chart()
    p = linker.inference.predict(threshold_match_probability=0.3)
    fid = "IT.CAED/880442001.FACILITY"
    fid = "IT.CAED/200662002.FACILITY"
    fid = "IT.CAED/100933004.FACILITY"
    filtered = (
        p.as_duckdbpyrelation()
        .filter(f"Facility_INSPIRE_ID_r = '{fid}'")
        .order("match_probability DESC")
        .df()
        .to_dict("records")
    )
    linker.visualisations.waterfall_chart(records=filtered)
