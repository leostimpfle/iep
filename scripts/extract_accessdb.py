"""Requires mdbtools https://github.com/mdbtools/mdbtools"""

import subprocess
from pathlib import Path

from iep.config import PATH_INPUT


def extract_tables(database: str | Path, output_dir: str | Path) -> None:
    """Extract all tables from an .accdb or .mdb file as CSVs."""
    database = Path(database)
    if not database.is_file():
        raise FileNotFoundError(f"Database not found: {database}")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = (
        subprocess.check_output(["mdb-tables", "-1", database], text=True)
        .strip()
        .split("\n")
    )
    for table in tables:
        if not table:
            continue
        raw = subprocess.check_output(["mdb-export", database, table], text=True)
        Path(output_dir, f"{table}.csv").write_text(raw)


# %% IEP
input_folder = Path(PATH_INPUT, "iep")
databases = input_folder.glob("*.accdb")
for database in databases:
    print(database.stem)
    output_folder = Path(database.with_suffix(""))
    output_folder.mkdir(exist_ok=True)
    extract_tables(database=database, output_dir=output_folder)

# %% E-PRTR
eprtr = Path(PATH_INPUT, "e-prtr", "E-PRTR_database_v18.mdb")
output = Path(eprtr.with_suffix(""))
output.mkdir(exist_ok=True)
extract_tables(database=eprtr, output_dir=output)

# %% LCP
lcp = Path(PATH_INPUT, "lcp", "LCP_database_v3.0.accdb")
output = Path(lcp.with_suffix(""))
output.mkdir(exist_ok=True)
extract_tables(database=lcp, output_dir=output)
