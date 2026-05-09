import duckdb
import splink
import splink.blocking_rule_library as br
import splink.comparison_level_library as cll
import splink.comparison_library as cl

import iep


def clean(column: str, remove: list[str] | None = None) -> str:
    expression = f"lower({column})"
    expression = f"trim(regexp_replace({expression}, '[^\\w\\s]', '', 'g'))"
    for r in remove or []:
        cleaned_r = f"trim(regexp_replace(lower({r}), '[^\\w\\s]', '', 'g'))"
        expression = f"replace({expression}, COALESCE({cleaned_r}, ''), '')"
    expression = f"trim({expression})"
    return f"NULLIF({expression}, '') AS {column}"


connection = duckdb.connect()
facilities = iep.facility.facility.load(connection=connection)
pollutants = iep.facility.pollutant_release._load_raw(
    reload=False, connection=connection
)
data = (
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
)
# fid = "CZ.MZP.J634/CZ0121803E.FACILITY"
fids = ("PL.MŚ/000000068.FACILITY", "PL.EEA/02C_000194.FACILITY")
fids = ("DK.EEA/6458.FACILITY", "DK.CAED/000096764.FACILITY")
facilities.filter(f"Facility_INSPIRE_ID in {fids}")

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
        cl.JaroWinklerAtThresholds("parentCompanyName").configure(
            term_frequency_adjustments=True
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
    input_table_or_tables=data,
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

linker.visualisations.match_weights_chart().save(
    r"/Users/leonardstimpfle/Downloads/deduplication.html"
)
prediction = linker.inference.predict(threshold_match_probability=0.75)
filtered = (
    prediction.as_duckdbpyrelation()
    # .filter(f"Facility_INSPIRE_ID_l IN {fids}")
    .filter(
        f"Facility_INSPIRE_ID_l = 'https://registry.gdi-de.org/id/de.st.lau.pf.anlagen-ied-euregistry/100125'"
    )
    .order("match_probability DESC")
    .df()
    .to_dict("records")
)
linker.visualisations.waterfall_chart(records=filtered)
