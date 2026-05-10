from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_IEP
from iep.io import NA_VALUES, read_duckdb
from iep.versions import VERSION


def load(
    version: str = VERSION,
    reload: bool = False,
    connection: DuckDBPyConnection = duckdb.default_connection(),
) -> DuckDBPyRelation:
    table_name: str = "4_ProductionInstallationPart"
    data = read_duckdb(
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
    return data
