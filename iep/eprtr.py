from pathlib import Path
from typing import Final

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_EPRTR
from iep.io import NA_VALUES, read_duckdb

_VERSION: Final[str] = "v18"


def load_reports(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "UPLOADEDREPORTS"
    return read_duckdb(
        fn=Path(PATH_EPRTR, f"E-PRTR_database_{version}", f"{table_name}.csv"),
        dtypes={
            "Code": "VARCHAR",
            "LOV_CountryID": "INTEGER",
            "ReportingYear": "INTEGER",
            "CurrentlyShown": "INTEGER",
            "RemarkText": "VARCHAR",
            "CdrUrl": "VARCHAR",
            "CdrUploaded": "DATETIME",
            "CdrReleased": "DATETIME",
            "ForReview": "DATETIME",
            "Published": "DATETIME",
            "ResubmitReason": "VARCHAR",
            "ImportedToEPRTRMaster": "DATETIME",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def _load_report_pollutantrelease(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name = "POLLUTANTRELEASEANDTRANSFERREPORT"
    return read_duckdb(
        fn=Path(PATH_EPRTR, f"E-PRTR_database_{version}", f"{table_name}.csv"),
        dtypes={
            "PollutantReleaseAndTransferReportID": "INTEGER",
            "CountryCode": "VARCHAR",
            "CountryName": "VARCHAR",
            "ReportingYear": "INTEGER",
            "CoordinateSystemCode": "VARCHAR",
            "CoordinateSystemName": "VARCHAR",
            "CdrReleased": "DATETIME",
            "Published": "DATETIME",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def _load_data_pollutantrelease(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name = "POLLUTANTRELEASE"
    return read_duckdb(
        fn=Path(PATH_EPRTR, f"E-PRTR_database_{version}", f"{table_name}.csv"),
        dtypes={
            "PollutantReleaseID": "INTEGER",
            "FacilityReportID": "INTEGER",
            "ReleaseMediumCode": "VARCHAR",
            "ReleaseMediumName": "VARCHAR",
            "PollutantCode": "VARCHAR",
            "PollutantName": "VARCHAR",
            "PollutantGroupCode": "VARCHAR",
            "PollutantGroupName": "VARCHAR",
            "PollutantCAS": "VARCHAR",
            "MethodBasisCode": "VARCHAR",
            "MethodBasisName": "VARCHAR",
            "TotalQuantity": "DOUBLE",
            "AccidentalQuantity": "DOUBLE",
            "UnitCode": "VARCHAR",
            "UnitName": "VARCHAR",
            "ConfidentialIndicator": "INTEGER",
            "ConfidentialityReasonCode": "VARCHAR",
            "ConfidentialityReasonName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_pollutantrelease(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = _load_data_pollutantrelease(
        version=version, reload=reload, connection=connection
    )
    report = _load_report_pollutantrelease(
        version=version, reload=reload, connection=connection
    )
    facility = _load_data_facility(
        version=version, reload=reload, connection=connection
    )
    data = data.join(
        facility.select(
            "FacilityID, FacilityReportID, PollutantReleaseAndTransferReportID"
        )
        .distinct()
        .join(
            report.select("PollutantReleaseAndTransferReportID, ReportingYear"),
            condition="PollutantReleaseAndTransferReportID",
            how="left",
        ),
        condition="FacilityReportID",
        how="left",
    )
    return data


def _load_report_facility(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name = "FACILITYID_CHANGES"
    return read_duckdb(
        fn=Path(PATH_EPRTR, f"E-PRTR_database_{version}", f"{table_name}.csv"),
        dtypes={
            "CountryCode": "VARCHAR",
            "ReportingYear": "INTEGER",
            "FacilityReportID": "INTEGER",
            "NationalID": "VARCHAR",
            "NewFacilityID": "INTEGER",
            "OldFacilityID": "INTEGER",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def _load_data_facility(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "FACILITYREPORT"
    return read_duckdb(
        fn=Path(PATH_EPRTR, f"E-PRTR_database_{version}", f"{table_name}.csv"),
        dtypes={
            "FacilityReportID": "INTEGER",
            "PollutantReleaseAndTransferReportID": "INTEGER",
            "FacilityID": "INTEGER",
            "NationalID": "VARCHAR",
            "ParentCompanyName": "VARCHAR",
            "FacilityName": "VARCHAR",
            "StreetName": "VARCHAR",
            "BuildingNumber": "VARCHAR",
            "City": "VARCHAR",
            "PostalCode": "VARCHAR",
            "CountryCode": "VARCHAR",
            "CountryName": "VARCHAR",
            "Lat": "DOUBLE",
            "Long": "DOUBLE",
            "RBDSourceCode": "VARCHAR",
            "RBDSourceName": "VARCHAR",
            "NUTSRegionSourceCode": "VARCHAR",
            "NUTSRegionSourceName": "VARCHAR",
            "NACEMainEconomicActivityCode": "VARCHAR",
            "NACEMainEconomicActivityName": "VARCHAR",
            "CompetentAuthorityName": "VARCHAR",
            "CompetentAuthorityAddressStreetName": "VARCHAR",
            "CompetentAuthorityAddressBuildingNumber": "INTEGER",
            "CompetentAuthorityAddressCity": "VARCHAR",
            "CompetentAuthorityAddressPostalCode": "INTEGER",
            "CompetentAuthorityAddressCountryCode": "VARCHAR",
            "CompetentAuthorityAddressCountryName": "VARCHAR",
            "CompetentAuthorityTelephoneCommunication": "VARCHAR",
            "CompetentAuthorityFaxCommunication": "VARCHAR",
            "CompetentAuthorityEmailCommunication": "VARCHAR",
            "CompetentAuthorityContactPersonName": "VARCHAR",
            "ProductionVolumeProductName": "VARCHAR",
            "ProductionVolumeQuantity": "INTEGER",
            "ProductionVolumeUnitCode": "VARCHAR",
            "ProductionVolumeUnitName": "VARCHAR",
            "TotalIPPCInstallationQuantity": "INTEGER",
            "OperatingHours": "INTEGER",
            "TotalEmployeeQuantity": "INTEGER",
            "WebsiteCommunication": "VARCHAR",
            "PublicInformation": "VARCHAR",
            "ConfidentialIndicator": "INTEGER",
            "ConfidentialityReasonCode": "VARCHAR",
            "ConfidentialityReasonName": "VARCHAR",
            "ProtectVoluntaryData": "INTEGER",
            "MainIASectorCode": "VARCHAR",
            "MainIASectorName": "VARCHAR",
            "MainIAActivityCode": "VARCHAR",
            "MainIAActivityName": "VARCHAR",
            "MainIASubActivityCode": "VARCHAR",
            "MainIASubActivityName": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def load_facility(
    version: str = _VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = _load_data_facility(version=version, reload=reload, connection=connection)
    reports = _load_report_facility(
        version=version, reload=reload, connection=connection
    )
    data = data.join(
        reports.select("CountryCode, FacilityReportID, ReportingYear"),
        condition="CountryCode, FacilityReportID",
        how="left",
    )
    return data
