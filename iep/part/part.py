from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import NA_VALUES, PATH_IEP, PATH_INPUT, VERSION
from iep.utils import read_duckdb
from iep.versions import stack_versions


def _load_raw(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "4_ProductionInstallationPart"
    return read_duckdb(
        fn=Path(PATH_IEP, version, f"{table_name}.csv"),
        dtypes={
            "fileId_EUReg": "INTEGER",
            "fileId_EPRTR_LCP": "INTEGER",
            "Parent_Installation_INSPIRE_ID": "VARCHAR",
            "Installation_Part_INSPIRE_ID": "VARCHAR",
            "ProductionInstallationPart_thematicId": "VARCHAR",
            "ProductionInstallationPart_thematicIdScheme": "VARCHAR",
            "nameOfFeature": "VARCHAR",
            "installationPartName_confidentialityReasonCode": "VARCHAR",
            "installationPartName_confidentialityReasonName": "VARCHAR",
            "plantType": "VARCHAR",
            "pointGeometryLat": "DOUBLE",
            "pointGeometryLon": "DOUBLE",
            "dateOfStartOfOperation": "DATETIME",
            "publicDisclosure": "VARCHAR",
            "combustionPlantCategory": "VARCHAR",
            "totalRatedThermalInput": "DOUBLE",
            "withinRefinery": "INTEGER",
            "untreatedMunicipalWaste": "INTEGER",
            "heatReleaseHazardousWaste": "INTEGER",
            "totalNominalCapacityAnyWasteType": "DOUBLE",
            "permittedCapacityHazardous": "DOUBLE",
            "permittedCapacityNonHazardous": "DOUBLE",
            "confidentialityReasonCode": "VARCHAR",
            "confidentialityReasonName": "VARCHAR",
            "publicDisclosureURL": "VARCHAR",
        },
        na_values=NA_VALUES,
        all_varchar=True,
        reload=reload,
        connection=connection,
    )


def _add_lcp(
    data: DuckDBPyRelation, connection: DuckDBPyConnection, reload: bool = False
) -> DuckDBPyRelation:
    import iep._lcp

    lcp_details = iep._lcp.load(connection=connection, reload=reload)
    lcp_links = connection.read_csv(PATH_INPUT / "links_lcp.csv")
    lcp_details = lcp_details.join(
        lcp_links, condition="Unique_Plant_ID", how="inner"
    ).aggregate(
        """Installation_Part_INSPIRE_ID,
        ReferenceYear AS reportingYear,
        SUM(MWth) AS totalRatedThermalInput,
        MAX(Refineries)::INTEGER AS withinRefinery
        """
    )
    data = connection.sql(
        """SELECT * FROM data
        UNION BY NAME
        SELECT * FROM lcp_details     
        """
    )
    return data


def load(
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    data = stack_versions(
        loader=_load_raw,
        aggregate_by={
            "reportingYear",
            "Parent_Installation_INSPIRE_ID",
            "Installation_Part_INSPIRE_ID",
        },
        reload=reload,
        connection=connection,
    )
    data = _add_lcp(data=data, connection=connection, reload=reload)
    return data
