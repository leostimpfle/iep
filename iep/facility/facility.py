from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_IEP, PATH_PACKAGE
from iep.identifiers import Level, _deduplicate
from iep.io import NA_VALUES, read_duckdb
from iep.versions import VERSION, stack_versions


def _load_raw(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "2_ProductionFacility"
    data = read_duckdb(
        fn=Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "Parent_Site_INSPIRE_ID": "VARCHAR",
            "Facility_INSPIRE_ID": "VARCHAR",
            "ProductionFacility_thematicId": "VARCHAR",
            "ProductionFacility_thematicIdScheme": "VARCHAR",
            "parentCompanyName": "VARCHAR",
            "parentCompanyURL": "VARCHAR",
            "parentCompany_confidentialityReasonCode": "VARCHAR",
            "parentCompany_confidentialityReasonName": "VARCHAR",
            "nameOfFeature": "VARCHAR",
            "facilityName_confidentialityReasonCode": "VARCHAR",
            "facilityName_confidentialityReasonName": "VARCHAR",
            "facilityType": "VARCHAR",
            "pointGeometryLat": "DOUBLE",
            "pointGeometryLon": "DOUBLE",
            "streetName": "VARCHAR",
            "buildingNumber": "VARCHAR",
            "city": "VARCHAR",
            "postalCode": "VARCHAR",
            "countryCode": "VARCHAR",
            "addressDetails_confidentialityReasonCode": "VARCHAR",
            "addressDetails_confidentialityReasonName": "VARCHAR",
            "mainActivityCode": "VARCHAR",
            "mainActivityName": "VARCHAR",
            "RBDSourceCode": "VARCHAR",
            "RBDSourceName": "VARCHAR",
            "NUTSRegionSourceCode": "VARCHAR",
            "NUTSRegionSourceName": "VARCHAR",
            "dateOfStartOfOperation": "DATETIME",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )
    return data


def _add_eprtr(
    data: DuckDBPyRelation,
    connection: DuckDBPyConnection,
    reload: bool = False,
) -> DuckDBPyRelation:
    from iep.eprtr import load_facility

    eprtr = (
        load_facility(reload=reload, connection=connection)
        .join(
            connection.read_csv(
                Path(PATH_PACKAGE, "facility", "links-eprtr.csv")
            ).aggregate(
                "FacilityID, FIRST(Facility_INSPIRE_ID ORDER BY match_weight DESC) AS Facility_INSPIRE_ID"
            ),
            condition="FacilityID",
            how="inner",
        )
        .select(
            "Facility_INSPIRE_ID, ReportingYear AS reportingYear, ParentCompanyName AS parentCompanyName, FacilityName AS nameOfFeature"
        )
    )
    connection.register("eprtr", eprtr)
    data = connection.sql(
        """
        SELECT * FROM data
        UNION BY NAME
        SELECT * FROM eprtr
        WHERE (Facility_INSPIRE_ID, reportingYear) NOT IN (
            SELECT Facility_INSPIRE_ID, reportingYear FROM data
        )
        """
    ).select(
        f"""Facility_INSPIRE_ID,
        reportingYear,
        {", ".join(f"COALESCE({c}, FIRST({c} IGNORE NULLS) OVER (PARTITION BY Facility_INSPIRE_ID ORDER BY reportingYear DESC)) AS {c}" for c in data.columns if not c in ["Facility_INSPIRE_ID", "reportingYear"])}
        """
    )
    return data


def load(
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = stack_versions(loader=_load_raw, reload=reload, connection=connection)
    data = _add_eprtr(data=data, connection=connection, reload=reload)
    data = _deduplicate(data=data, connection=connection, level=Level.Facility)
    return data
