from pathlib import Path
from typing import Final

PATH_PACKAGE: Final[Path] = Path(__file__).resolve().parent
PATH_INPUT: Final[Path] = PATH_PACKAGE.parent / "input"
VERSION: Final[str] = "v16"
