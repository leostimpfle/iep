import hashlib
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

    def to_queue(self) -> CteQueue:
        queue = CteQueue()
        return queue.extend(name=self.name, query=self.query)


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

    @property
    def hash(self) -> str:
        hash = hashlib.sha256(self.query.encode()).hexdigest()[:8]
        return f"__{hash}"

    @property
    def query(self) -> str:
        return ", ".join(cte.to_sql() for cte in self.ctes)

    def to_sql(self, recursive: bool = False, table: str | None = None) -> str:
        keyword = "WITH RECURSIVE" if recursive else "WITH"
        if table is None:
            table = self.final
        return f"{keyword} {self.query} SELECT * FROM {table}"


def balance(data: CteQueue, time: str, groups: list[str]) -> CteQueue:
    input_name = data.final
    prefix: str = data.hash
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


def is_unit_error(
    table: str,
    time: str,
    identifiers: list[str],
    value: str,
    threshold_delta: float = 0.5,
    threshold_range: float = 0.5,
) -> str:
    return dedent(
        f"""SELECT
            *,
            MIN({value}) OVER w_unordered AS _min,
            MAX({value}) OVER w_unordered AS _max, 
            LAG({value}) OVER w_ordered AS _lag,
            LEAD({value}) OVER w_ordered AS _lead,
            CASE
                WHEN {value} > 0.0 AND _lag > 0.0 THEN LOG10({value} / _lag)
            END AS _delta,
            CASE
                WHEN {value} > 0.0 AND _lead > 0.0 THEN LOG10(_lead / {value})
            END AS _delta_lead,
            NULLIF(ROUND(_delta), 0) AS scalar,
            scalar IS NOT NULL
                AND _delta IS NOT NULL AND ABS(_delta) > {threshold_delta}
                AND _delta_lead IS NOT NULL AND ABS(_delta_lead) > {threshold_delta}
                AND SIGN(_delta) != SIGN(_delta_lead)
                AND {value} / POW(10, scalar) 
                    BETWEEN _min * (1 - {threshold_range}) AND _max * (1 + {threshold_range}) 
            AS error
        FROM {table} 
        WINDOW
            w_unordered AS (PARTITION BY {", ".join(identifiers)}),
            w_ordered AS (PARTITION BY {", ".join(identifiers)} ORDER BY {time})
        """
    )


def sanitise_units(
    data: CteQueue,
    value: str,
    time: str,
    groups: list[str],
    threshold: float = 0.5,
    permissible_range: float = 0.5,
) -> CteQueue:
    """Correct probable unit errors by rescaling outlier emissions.

    A unit error (e.g. kg entered as g or tonnes) is identified as a spike
    that reverts the following year:
        - log-change exceeds `threshold`
        - reversed in the next year
        - rescaled value falls within `permissible_range` of the facility's overall emissions
    """
    input: str = data.final
    prefix: str = data.hash
    data = data.extend(
        name=f"{prefix}_scalar",
        query=is_unit_error(
            table=input,
            time=time,
            identifiers=groups,
            value=value,
            threshold_delta=threshold,
            threshold_range=permissible_range,
        ),
    )
    data = data.extend(
        name=f"{prefix}_sanitised_units",
        query=dedent(
            f"""SELECT
                t.* REPLACE(
                    CASE
                        WHEN s.error THEN t.{value} / POW(10, s.scalar)
                        ELSE t.{value}
                    END AS {value}
                )
            FROM {input} t
            LEFT JOIN {prefix}_scalar s
            ON {" AND ".join(f"t.{c} IS NOT DISTINCT FROM s.{c}" for c in groups)}
            AND t.{time} = s.{time}
            """
        ),
    )
    return data


# def get_proxy(data: CteQueue, value: str, time: str, groups: list[str]) -> CteQueue:
#     data = data.extend(
#         name=data.hash,
#         query=dedent(
#             f"""SELECT
#                 {time},
#                 HASH({", ".join(groups)}) AS proxy_id,
#                 SUM({value}) AS proxy_value
#             FROM {data.final}
#             GROUP BY ALL
#             """
#         ),
#     )
#     return data


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
