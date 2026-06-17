import math
from enum import StrEnum
from pathlib import Path
from typing import Final

PATH_PACKAGE: Final[Path] = Path(__file__).resolve().parent
PATH_INPUT: Final[Path] = PATH_PACKAGE / "_input"
PATH_IEP: Final[Path] = PATH_INPUT / "iep"
PATH_EPRTR: Final[Path] = PATH_INPUT / "eprtr"
PATH_LCP: Final[Path] = PATH_INPUT / "lcp"


class Version(StrEnum):
    v1 = "1215_Public_Product_Full Access_template_v16_public"
    v3 = "Industrial Reporting Database v3 December 2020"
    v4 = "1215_Public_Product_Full Access_draft_v19_mapping_15_12_21"
    v6 = "1215_Public_Product_Full Access_draft_v19_April_2022_v6"
    v8 = "1215_Public_Product_Full Access_v8"
    v9 = "1215_Public_Product_Full Access_v9_May_2023"
    v10 = "Industrial_dataset_v10_December_2023"
    v11 = "Industrial_dataset_v_11_2024_07_10"
    v12 = "Industrial_dataset_v_12_2024_09_10"
    v13 = "Industrial_dataset_v_13_2024_12_16"
    v14 = "Industrial_dataset_v_14_2025_03_10"
    v15 = "Industrial_dataset_v_15_2025_12_15"
    v16 = "Industrial_dataset_v_16_2026_02_16"


VERSION: Final[str] = Version.v16.value

NA_VALUES: Final[list[str | int | float]] = [
    "CONFIDENTIAL",
    "None",
    "none specified",
    "does not exist",
    "UNNAMED ROAD",
    "n.a.",
    "_",
    "-",
    "--",
    "x",
    "01/00/00 00:00:00",
    "01/01/00 00:00:00",
    "12/31/99 00:00:00",
    "01/02/00 00:00:00",
]

# Almost a factor of 1_000 do catch obvious misreporting of kg as tonnes or grams
THRESHOLD_UNIT_ERROR: Final[float] = math.log10(900)
THRESHOLD_RANGE: Final[float] = 0.5
