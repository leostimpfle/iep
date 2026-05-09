"""Requires mdbtools https://github.com/mdbtools/mdbtools"""

import subprocess
from pathlib import Path

input_folder = Path(Path(__file__).parents[1], "input", "iep")
databases = input_folder.glob("*.accdb")
for database in databases:
    print(database.stem)
    output_folder = Path(database.with_suffix(""))
    output_folder.mkdir(exist_ok=True)
    tables = (
        subprocess.check_output(["mdb-tables", "-1", database], text=True)
        .strip()
        .split("\n")
    )
    for table in tables:
        if not table:
            continue
        print(table)
        raw = subprocess.check_output(["mdb-export", database, table], text=True)
        Path(output_folder, f"{table}.csv").write_text(raw)
