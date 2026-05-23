from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum, auto
from pathlib import Path
from textwrap import dedent
from typing import Literal

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation


@dataclass(kw_only=True, frozen=True, slots=True)
class Cte:
    name: str
    query: str

    def to_sql(self) -> str:
        return f"{self.name} AS ({self.query})"


@dataclass(kw_only=True, frozen=True, slots=True)
class CteQueue:
    ctes: tuple[Cte, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        duplicates = {
            cte_name
            for cte_name, count in Counter(cte.name for cte in self.ctes).items()
            if count > 1
        }
        if duplicates:
            raise ValueError(f"Duplicate CTE names: {duplicates}")

    def extend(self, name: str, query: str) -> "CteQueue":
        return CteQueue(ctes=self.ctes + (Cte(name=name, query=query),))

    @property
    def n(self) -> int:
        return len(self.ctes)

    @property
    def final(self) -> str:
        return self.ctes[-1].name

    def to_sql(self, recursive: bool = False) -> str:
        keyword = "WITH RECURSIVE" if recursive else "WITH"
        return f"{keyword} {', '.join(cte.to_sql() for cte in self.ctes)} SELECT * FROM {self.final}"


def balance(data: CteQueue, time: str, groups: list[str]) -> CteQueue:
    input_name = data.final
    prefix: str = "_balance"
    data = data.extend(
        name=f"{prefix}_identifiers",
        query=dedent(f"""SELECT DISTINCT {", ".join(groups)} FROM {input_name}"""),
    )
    data = data.extend(
        name=f"{prefix}_periods",
        query=dedent(
            f"""SELECT
                unnest(
                    generate_series(
                        (SELECT MIN("{time}") FROM base),
                        (SELECT MAX("{time}") FROM base)
                    )
                ) AS "{time}"
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_balanced",
        query=dedent(
            f"""SELECT
                i.*, 
                p.*,
                b.* EXCLUDE({", ".join(groups)}, "{time}")
            FROM {prefix}_identifiers AS i
            CROSS JOIN {prefix}_periods AS p
            LEFT JOIN {input_name} b
            ON {" AND ".join(f"b.{c} = i.{c}" for c in groups)}
            AND b."{time}" = p."{time}"
            """
        ),
    )
    return data


class DuckDBReader(StrEnum):
    read_xlsx = auto()
    read_csv = auto()


def _cast(
    column: str,
    dtype: str,
    na_values: list[str | int | float] | None = None,
    date_format: str = "%m/%d/%y %H:%M:%S",
    errors: Literal["raise", "coerce"] = "coerce",
) -> str:
    cast = "CAST" if errors == "raise" else "TRY_CAST"
    column = f'"{column}"'
    if dtype in ("DATETIME", "TIMESTAMP") and date_format is not None:
        query_column = f"""STRPTIME({column}, '{date_format}')"""
        query_column = f"""CASE
            WHEN {query_column} > CURRENT_DATE THEN {query_column} - INTERVAL '100 years'
            ELSE {query_column}
        END"""
    else:
        query_column = column
    query = f"""{cast}({query_column} AS {dtype})"""
    if na_values is not None:
        # enclose strings in quotes but otherwise take value
        formatted = (f"'{v}'" if isinstance(v, str) else str(v) for v in na_values)
        query = f"""CASE WHEN {column} IN ({", ".join(formatted)}) THEN NULL ELSE {query} END"""

    return f"{query} AS {column}"


def read_duckdb(
    fn: Path,
    dtypes: dict[str, str],
    na_values: list[str | int | float] | None = None,
    date_format: str = "%m/%d/%y %H:%M:%S",
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
            _cast(
                column=column, dtype=dtype, na_values=na_values, date_format=date_format
            )
            for column, dtype in dtypes.items()
        )
        arguments = (f"'{fn}'", *(f'{k} = "{v}"' for k, v in kwargs.items()))
        data = connection.sql(
            f"""SELECT {columns} FROM {reader}({", ".join(arguments)})"""
        )
        data.write_parquet(cache.as_posix())
        return data
