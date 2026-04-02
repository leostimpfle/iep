from enum import StrEnum, auto
from pathlib import Path
from typing import Literal

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation


class DuckDBReader(StrEnum):
    read_xlsx = auto()
    read_csv = auto()


def _cast(
    column: str,
    dtype: str,
    na_values: list[str | int | float] | None = None,
    errors: Literal["raise", "coerce"] = "coerce",
) -> str:
    cast = "CAST" if errors == "raise" else "TRY_CAST"
    if dtype == "DATE_EXCEL":
        query = f'''CASE WHEN "{column}" IS NULL OR NOT {cast}("{column}" AS INTEGER) BETWEEN 2 AND 50000
            THEN NULL
            ELSE CAST('1899-12-30' AS DATE) + {cast}("{column}" AS INTEGER)
        END'''
    else:
        query = f'''{cast}("{column}" AS {dtype})'''
    if na_values is not None:
        # enclose strings in quotes but otherwise take value
        formatted = (f"'{v}'" if isinstance(v, str) else str(v) for v in na_values)
        query = f"""CASE WHEN "{column}" IN ({", ".join(formatted)}) THEN NULL ELSE {query} END"""

    return f'{query} AS "{column}"'


def read_duckdb(
    fn: Path,
    dtypes: dict[str, str],
    na_values: list[str | int | float] | None = None,
    *,
    reload: bool = False,
    cache: Path | None = None,
    reader: DuckDBReader | None = None,
    connection: DuckDBPyConnection = duckdb.default_connection(),
    **kwargs,
) -> DuckDBPyRelation:
    if cache is None:
        cache = fn.with_suffix(".parquet")
    if cache.exists() and not reload:
        return connection.sql(f"""SELECT * FROM read_parquet('{cache}')""")
    else:
        if reader is None:
            # infer reader from file extension
            reader = DuckDBReader(f"read_{fn.suffix.replace('.', '')}")
        columns = ", ".join(
            _cast(column=column, dtype=dtype, na_values=na_values)
            for column, dtype in dtypes.items()
        )
        arguments = (f"'{fn}'", *(f'{k} = "{v}"' for k, v in kwargs.items()))
        data = connection.sql(
            f"""SELECT {columns} FROM {reader}({", ".join(arguments)})"""
        )
        data.write_parquet(cache.as_posix())
        return data
