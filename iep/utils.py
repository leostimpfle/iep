from collections import Counter
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum, auto
from pathlib import Path
from textwrap import dedent
from typing import Iterable, Literal

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_PACKAGE


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

    def to_sql(self, recursive: bool = False, table: str | None = None) -> str:
        keyword = "WITH RECURSIVE" if recursive else "WITH"
        if table is None:
            table = self.final
        return f"{keyword} {', '.join(cte.to_sql() for cte in self.ctes)} SELECT * FROM {table}"


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
                        (SELECT MIN("{time}") FROM {input_name}),
                        (SELECT MAX("{time}") FROM {input_name})
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
            ON {" AND ".join(f"b.{c} IS NOT DISTINCT FROM i.{c}" for c in groups)}
            AND b."{time}" = p."{time}"
            """
        ),
    )
    return data


def sanitise_units(
    data: CteQueue,
    value: str,
    time: str,
    groups: Iterable[str],
    threshold: int = 800,
    permissible_range: float = 0.5,
) -> CteQueue:
    """Correct probable unit errors by rescaling outlier emissions.

    A unit error (e.g. kg entered as g or tonnes) is identified as a spike
    that reverts the following year:
        - log-change exceeds `threshold`
        - reversed in the next year
        - rescaled value falls within `permissible_range` of the facility's overall emissions
    """
    input_name: str = data.final
    prefix: str = "_sanitise_units"
    data = data.extend(
        name=f"{prefix}_base",
        query=dedent(
            f"""SELECT
                {time},
                {", ".join(groups)},
                FIRST({value}) AS val 
            FROM {input_name} 
            GROUP BY ALL
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_stats",
        query=dedent(
            f"""SELECT
                *,
                MIN(val) OVER w  AS _min,
                MAX(val) OVER w AS _max,
                LAG(val) OVER w_ordered AS lagged,
                val > 0 AND lagged > 0 AS valid,
                CASE
                    WHEN COALESCE(valid, FALSE) 
                    THEN LOG10(val / lagged)
                END AS log_change,
                CASE
                    WHEN COALESCE(valid, FALSE)
                    THEN ROUND(LOG10(val))::BIGINT
                END AS order_of_magnitude
            FROM {prefix}_base
            WINDOW
                w AS (PARTITION BY {", ".join(groups)}),
                w_ordered AS (
                    PARTITION by {", ".join(groups)} 
                    ORDER BY {time} 
                )
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_scalar",
        query=dedent(
            f"""SELECT
                *,
                POW(10, order_of_magnitude - LEAD(order_of_magnitude) OVER w)::DOUBLE AS scalar 
            FROM {prefix}_stats 
            WINDOW w AS (
                PARTITION BY {", ".join(groups)} 
                ORDER BY {time} 
            )
            QUALIFY
                -- only large change 
                ABS(log_change) > log10({threshold})
                -- only adjust if change reverts in following year
                AND ABS(LEAD(log_change) OVER w) > log10({threshold})
                AND SIGN(log_change) != SIGN(LEAD(log_change) OVER w)
                -- require that scaled emissions are within permissible range
                AND val / POW(10, order_of_magnitude - LEAD(order_of_magnitude) OVER w) 
                    BETWEEN _min * (1 - {permissible_range}) AND _max * (1 + {permissible_range})
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_{input_name}",
        query=dedent(
            f"""SELECT
                t.* REPLACE(
                    t.{value} / COALESCE(s.scalar, 1) AS {value}
                )
            FROM {input_name} t
            LEFT JOIN {prefix}_scalar s
            ON {" AND ".join(f"t.{c} IS NOT DISTINCT FROM s.{c}" for c in groups)} AND t.{time} = s.{time}"""
        ),
    )
    return data


class Level(IntEnum):
    Site = 1
    Facility = 2
    Installation = 3
    Installation_Part = 4


def deduplicate(data: CteQueue, level: Level) -> CteQueue:
    input_name: str = data.final
    data = data.extend(
        name="_deduplication",
        query=dedent(
            f"""SELECT DISTINCT
                {level.name}_INSPIRE_ID_cluster,
                {level.name}_INSPIRE_ID
            FROM read_csv('{Path(PATH_PACKAGE, level.name.lower(), "deduplication.csv")}')
            """
        ),
    )
    data = data.extend(
        name=f"_deduplication_{input_name}",
        query=dedent(
            f"""SELECT
                l.* REPLACE(
                    COALESCE(
                        r.{level.name}_INSPIRE_ID_cluster,
                        l.{level.name}_INSPIRE_ID
                    ) AS {level.name}_INSPIRE_ID
                )
            FROM {input_name} l
            LEFT JOIN _deduplication r
            USING ({level.name}_INSPIRE_ID)
            """
        ),
    )
    return data
