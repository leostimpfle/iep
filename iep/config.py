import math
from pathlib import Path
from typing import Final

PATH_PACKAGE: Final[Path] = Path(__file__).resolve().parent
PATH_INPUT: Final[Path] = PATH_PACKAGE / "_input"
PATH_IEP: Final[Path] = PATH_INPUT / "iep"
PATH_EPRTR: Final[Path] = PATH_INPUT / "eprtr"

VERSION: Final[str] = "Industrial_dataset_v_16_2026_02_16"

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
