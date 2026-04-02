from typing import Final, Literal

type Layout = Literal["wide", "long"]


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
    "1900-01-01 00:00:00",
]
