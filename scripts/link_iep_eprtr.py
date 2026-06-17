from pathlib import Path
from textwrap import dedent
from typing import Final

import duckdb
import splink
import splink.blocking_rule_library as br
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from _duckdb import DuckDBPyConnection, DuckDBPyRelation

import iep
from iep._eprtr import load_facility as load_eprtr_facility
from iep._eprtr import load_pollutantrelease as load_eprtr_pollutantrelease
from iep.config import PATH_PACKAGE, VERSION
from iep.utils import clean

_START_YEAR: Final[int] = 2007
_END_YEAR: Final[int] = 2017
_POLLUTANTS: Final[list[tuple[str, str]]] = [("AIR", "CO2"), ("AIR", "NOX")]
_N: Final[int] = (_END_YEAR - _START_YEAR + 1) * len(_POLLUTANTS)


def standardise() -> str:
    columns = ["streetName", "buildingNumber", "city", "postalCode"]
    return dedent(
        f"""
        facility_id,
        countryCode,
        pointGeometryLat,
        pointGeometryLon,
        {clean("parentCompanyName", remove=["city"])} AS parentCompanyName,
        {clean("nameOfFeature", remove=["city", "parentCompanyName"])} AS nameOfFeature,
        {clean("mainActivityCode")} AS mainActivityCode,
        {", ".join(f"trim(lower({c})) AS {c}" for c in columns)},
        releases,
        total_releases
        """
    )


def _load_iep(
    connection: DuckDBPyConnection = duckdb.default_connection(),
    version: str = VERSION,
) -> DuckDBPyRelation:
    facilities = iep.facility.facility._load_raw(
        version=version, connection=connection
    ).select("*, Facility_INSPIRE_ID AS facility_id")
    pollutants = (
        iep.facility.pollutant_release.load(version=version, connection=connection)
        .filter("reportingYear <= 2017")
        .aggregate(
            "Facility_INSPIRE_ID, reportingYear, medium, pollutantCode, MAX(totalPollutantQuantityKg) AS totalPollutantQuantityKg"
        )
    )
    pollutants = connection.sql(
        dedent(
            f"""WITH complete AS (
                SELECT
                    f.Facility_INSPIRE_ID,
                    y.reportingYear,
                    p.pollutantCode,
                    p.medium,
                    COALESCE(totalPollutantQuantityKg, 0.0) AS totalPollutantQuantityKg
                FROM (SELECT DISTINCT Facility_INSPIRE_ID FROM pollutants) f
                CROSS JOIN (SELECT unnest(generate_series({_START_YEAR}, {_END_YEAR})) AS reportingYear) y
                CROSS JOIN (SELECT * FROM (VALUES {", ".join(str(p) for p in _POLLUTANTS)}) AS t(medium, pollutantCode)) p
                --CROSS JOIN (SELECT DISTINCT medium, pollutantCode FROM pollutants) p
                LEFT JOIN pollutants USING (Facility_INSPIRE_ID, reportingYear, pollutantCode, medium)  
            )
            SELECT
                Facility_INSPIRE_ID,
                list(totalPollutantQuantityKg ORDER BY reportingYear, medium, pollutantCode)::DOUBLE[{_N}] AS releases,
                SUM(totalPollutantQuantityKg) AS total_releases
            FROM complete 
            GROUP BY Facility_INSPIRE_ID
            """
        )
    )
    facilities = facilities.join(
        pollutants, condition="Facility_INSPIRE_ID", how="left"
    )
    return facilities.select(standardise())


def _load_eprtr(
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    facilities = load_eprtr_facility(connection=connection)
    by = ["FacilityID", "CountryCode", "NationalID"]
    columns = [
        f"LAST({c} ORDER BY ReportingYear) AS {c}"
        for c in facilities.columns
        if c not in by
    ]
    facilities = facilities.aggregate(
        f"FacilityID, CountryCode AS countryCode, NationalID, {', '.join(columns)}"
    ).select(
        """*,
        ParentCompanyName AS parentCompanyName,
        FacilityName AS nameOfFeature,
        StreetName AS streetName,
        BuildingNumber AS buildingNumber,
        City AS city,
        PostalCode AS postalCode,
        Lat AS pointGeometryLat,
        Long AS pointGeometryLon,
        MainIAActivityCode AS mainActivityCode,
        MainIASectorCode AS NACEMainEconomicActivityCode
        """
    )
    pollutants = load_eprtr_pollutantrelease(connection=connection).aggregate(
        "FacilityID, ReportingYear, PollutantCode, ReleaseMediumCode, MAX(TotalQuantity) AS TotalQuantity",
    )
    pollutants = connection.sql(
        dedent(
            f"""WITH complete AS (
                SELECT
                    f.FacilityID,
                    y.ReportingYear,
                    p.PollutantCode,
                    p.ReleaseMediumCode,
                    COALESCE(TotalQuantity, 0.0) AS TotalQuantity
                FROM (SELECT DISTINCT FacilityID FROM pollutants) f
                CROSS JOIN (SELECT unnest(generate_series({_START_YEAR}, {_END_YEAR})) AS ReportingYear) y
                CROSS JOIN (SELECT * FROM (VALUES {", ".join(str(p) for p in _POLLUTANTS)}) AS t(ReleaseMediumCode, PollutantCode)) p
                --CROSS JOIN (SELECT DISTINCT ReleaseMediumCode, PollutantCode FROM pollutants) p
                LEFT JOIN pollutants USING (FacilityID, ReportingYear, PollutantCode, ReleaseMediumCode)  
            )
            SELECT
                FacilityID,
                list(TotalQuantity ORDER BY ReportingYear, ReleaseMediumCode, PollutantCode)::DOUBLE[{_N}] AS releases,
                SUM(TotalQuantity) AS total_releases
            FROM complete 
            GROUP BY FacilityID 
            """
        )
    )
    facilities = facilities.join(pollutants, condition="FacilityID", how="left")
    return facilities.select("*, FacilityID AS facility_id").select(standardise())


# pollutants = load_eprtr_pollutantrelease()
connection = duckdb.connect()
iep_data = _load_iep(connection=connection)
eprtr_data = _load_eprtr(connection=connection)

settings = splink.SettingsCreator(
    link_type="link_only",
    unique_id_column_name="facility_id",
    retain_intermediate_calculation_columns=True,
    blocking_rules_to_generate_predictions=[
        br.block_on("countryCode", "city"),
        br.block_on("countryCode", "postalCode"),
        br.block_on("countryCode", "parentCompanyName"),
        br.block_on("countryCode", "nameOfFeature"),
        # br.block_on("countryCode", "mainActivityCode"),
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
                        cll.ExactMatchLevel("streetName"),
                        cll.ExactMatchLevel("buildingNumber"),
                        cll.ExactMatchLevel("city"),
                        cll.ExactMatchLevel("postalCode"),
                    ),
                    cll.DistanceInKMLevel(
                        lat_col="pointGeometryLat",
                        long_col="pointGeometryLon",
                        km_threshold=0.1,
                    ),
                ).configure(label_for_charts="Exact"),
                *(
                    cll.DistanceInKMLevel(
                        lat_col="pointGeometryLat",
                        long_col="pointGeometryLon",
                        km_threshold=d,
                    )
                    for d in [1.0, 5.0, 10.0, 25.0, 50.0]
                ),
                cll.ElseLevel(),
            ],
        ),
        cl.ExactMatch("mainActivityCode").configure(term_frequency_adjustments=True),
        cl.CustomComparison(
            output_column_name="Releases",
            comparison_levels=[
                cll.NullLevel("releases"),
                *(
                    cll.CustomLevel(
                        sql_condition=f"""
                        array_cosine_similarity(releases_l, releases_r) > {threshold}
                        AND LEAST(total_releases_l, total_releases_r) / GREATEST(total_releases_l, total_releases_r) > {threshold} 
                        """,
                    )
                    for threshold in (0.999, 0.99, 0.95, 0.7, 0.5)
                ),
                cll.ElseLevel(),
            ],
        ),
    ],
)

linker = splink.Linker(
    input_table_or_tables=[iep_data, eprtr_data],  # ty:ignore[invalid-argument-type]
    settings=settings,
    db_api=splink.DuckDBAPI(connection=connection),
)
linker.training.estimate_probability_two_random_records_match(
    deterministic_matching_rules=[
        splink.block_on(
            "countryCode",
            "parentCompanyName",
            "nameOfFeature",
            "streetName",
            "buildingNumber",
            "city",
            "postalCode",
            # "mainActivityCode",
        ),
    ],
    recall=0.7,
)
linker.training.estimate_u_using_random_sampling(max_pairs=1e8)
for rule in [
    splink.block_on("countryCode", "postalCode"),
    splink.block_on("countryCode", "parentCompanyName"),
    splink.block_on("countryCode", "nameOfFeature"),
    splink.block_on("countryCode", "city"),
]:
    linker.training.estimate_parameters_using_expectation_maximisation(
        blocking_rule=rule,
    )


prediction = linker.inference.predict(threshold_match_probability=0.95)
prediction.as_duckdbpyrelation().aggregate(
    """facility_id_l AS Facility_INSPIRE_ID,
    FIRST(facility_id_r ORDER BY match_weight DESC) AS FacilityID,
    FIRST(match_weight ORDER BY match_weight DESC) AS match_weight,
    FIRST(match_probability ORDER BY match_weight DESC) AS match_probability
    """
).order("Facility_INSPIRE_ID").to_csv(
    Path(PATH_PACKAGE, "facility", "links-eprtr.csv").as_posix()
)

# %% evaluation
linker.visualisations.match_weights_chart()
fid = "https://registry.gdi-de.org/id/de.nw.inspire.pf.bube-eureg/arb-2017-978024-900-9103527"
fid = "https://registry.gdi-de.org/id/de.st.lau.pf.anlagen-ied-euregistry/100125"
filtered = (
    prediction.as_duckdbpyrelation()
    # .filter(f"Facility_INSPIRE_ID_l IN {fids}")
    .filter(f"facility_id_l = '{fid}'")
    .order("match_probability DESC")
    .df()
    .to_dict("records")
)
linker.visualisations.waterfall_chart(records=filtered)
