import hashlib
from collections import Counter
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum, auto
from pathlib import Path
from textwrap import dedent
from typing import Literal

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.config import PATH_PACKAGE, THRESHOLD_RANGE, THRESHOLD_UNIT_ERROR


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

    @classmethod
    def from_query(cls, name: str, query: str) -> "CteQueue":
        cte = Cte(name=name, query=query)
        return CteQueue(ctes=(cte,))

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


def fill(
    column: str,
    partition_by: list[str],
    direction: Literal["back", "forward", "both"] = "both",
    order_by: list[str] | None = None,
) -> str:
    partition = f"PARTITION BY {', '.join(partition_by)}"
    order = f"ORDER BY {', '.join(order_by)}" if order_by is not None else ""
    backfill: str = dedent(
        f"""FIRST_VALUE({column} IGNORE NULLS) OVER (
            {partition}
            {order}
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        )"""
    )
    forwardfill: str = dedent(
        f"""LAST_VALUE({column} IGNORE NULLS) OVER (
            {partition}
            {order}
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )"""
    )
    if direction == "both":
        return dedent(f"""COALESCE({column}, {backfill}, {forwardfill})""")
    elif direction == "back":
        return dedent(f"""COALESCE({column}, {backfill})""")
    elif direction == "forward":
        return dedent(f"""COALESCE({column}, {forwardfill})""")
    else:
        raise ValueError(f"Unknown direction: {direction}")


def is_outlier(
    data: CteQueue,
    table: str,
    time: str,
    identifiers: list[str],
    groups: list[str],
    reference: str,
    target: str,
    threshold_quantile: float = 0.99,
    threshold_outlier: float = 2.0,
    window: int = 2,
) -> CteQueue:
    """Flag `target` outliers based on two conditions:

    `target`
        deviates extremely form within-unit median (`threshold_outlier`)
    `reference`
        deviates extremely from within-unit median (`threshold_outlier`)
        or outside quantile range (`threshold_quantile`)
    """
    prefix: str = data.hash
    data = data.extend(
        name=f"{prefix}_quantile",
        query=dedent(
            f"""SELECT
                *,
                QUANTILE_CONT({reference}, {threshold_quantile})
                    FILTER({reference} > 0.0)
                    OVER w_sample
                AS _quantile_upper,
                QUANTILE_CONT({reference}, {1 - threshold_quantile})
                    FILTER({reference} > 0.0)
                    OVER w_sample
                AS _quantile_lower,
                -- reference outside of quantiles of full sample 
                {reference} BETWEEN _quantile_lower AND _quantile_upper is_in_range
            FROM {table}
            WINDOW
                w_sample AS (PARTITION BY {", ".join(groups + [time])})
            """
        ),
    )
    data = data.extend(
        name=f"{prefix}_threshold",
        query=dedent(
            # Calculate reference and target median (take local if available, otherwise global)
            f"""SELECT
                *,
                MEDIAN({reference}) FILTER(is_in_range) OVER w_global AS {reference}_median_global,
                MEDIAN({target}) FILTER(is_in_range) OVER w_global AS {target}_median_global,
                MEDIAN({reference}) FILTER(is_in_range) OVER w_local AS {reference}_median_local,
                MEDIAN({target}) FILTER(is_in_range) OVER w_local AS {target}_median_local,
                COALESCE({reference}_median_local, {reference}_median_global) AS {reference}_median,
                COALESCE({target}_median_local, {target}_median_global) AS {target}_median
            FROM {prefix}_quantile
            WINDOW
                w_global AS (
                    PARTITION BY {", ".join(identifiers + groups)}
                ),
                w_local AS (
                    PARTITION BY {", ".join(identifiers + groups)}
                    ORDER BY {time} 
                    ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
                )
            """
        ),
    )
    data = data.extend(
        name=f"{table}_outlier",
        query=dedent(
            f"""SELECT
                *,
                LAG({reference}) OVER w_unit AS {reference}_lag,
                LEAD({reference}) OVER w_unit AS {reference}_lead,
                0.1 / {threshold_outlier} AS ratio_floor,  -- this captures zeroes
                CASE
                    WHEN {reference} NOT NULL AND {reference}_median > 0.0
                    THEN LOG10(GREATEST({reference} / {reference}_median, ratio_floor))
                END AS {reference}_to_median, 
                CASE
                    WHEN {target} NOT NULL AND {target}_median > 0.0
                    THEN LOG10(GREATEST({target} / {target}_median, ratio_floor))
                END AS {target}_to_median, 
                -- reference much larger than median within unit
                ABS({reference}_to_median) > LOG10({threshold_outlier}) AS is_reference_outlier, 
                -- target much larger than median within unit
                ABS({target}_to_median) > LOG10({threshold_outlier}) AS is_target_outlier,
                CASE
                   WHEN is_target_outlier AND (is_reference_outlier OR NOT is_in_range)
                   THEN NULLIF(ROUND({reference}_to_median), 0) 
                END AS scalar
            FROM {prefix}_threshold
            WINDOW w_unit AS (
                PARTITION BY {", ".join(identifiers + groups)}
                ORDER BY {time}
                ROWS BETWEEN UNBOUNDED PRECEDING AND {window} FOLLOWING
                EXCLUDE CURRENT ROW
            )
            """
        ),
    )
    return data


def is_jump(
    table: str,
    time: str,
    identifiers: list[str],
    value: str,
    threshold_delta: float,
    threshold_range: float,
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
            AS is_jump
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
    threshold_delta: float = THRESHOLD_UNIT_ERROR,
    threshold_range: float = THRESHOLD_RANGE,
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
        query=is_jump(
            table=input,
            time=time,
            identifiers=groups,
            value=value,
            threshold_delta=threshold_delta,
            threshold_range=threshold_range,
        ),
    )
    data = data.extend(
        name=f"{prefix}_sanitised_units",
        query=dedent(
            f"""SELECT
                t.* REPLACE(
                    CASE
                        WHEN s.is_jump THEN t.{value} / POW(10, s.scalar)
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


def interpolate(
    data: CteQueue,
    table: str,
    time: str,
    identifiers: list[str],
    groups: list[str],
    target: str,
) -> CteQueue:
    data = data.extend(
        name=f"{table}_bounds",
        query=dedent(
            f"""SELECT
                *,
                LAST_VALUE({target} IGNORE NULLS) OVER (
                    w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS {target}_previous,
                LAST_VALUE(
                    CASE WHEN {target} IS NOT NULL THEN {time} END IGNORE NULLS
                ) OVER (
                    w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS {time}_previous,
                FIRST_VALUE({target} IGNORE NULLS) OVER (
                    w ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ) AS {target}_next,
                FIRST_VALUE(
                    CASE WHEN {target} IS NOT NULL THEN {time} END IGNORE NULLS
                ) OVER (
                    w ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ) AS {time}_next 
            FROM {table}
            WINDOW
                w AS (
                    PARTITION BY {", ".join(identifiers + groups)} 
                    ORDER BY {time} 
                )
            """
        ),
    )
    data = data.extend(
        name=f"{table}_interpolated",
        query=dedent(
            f"""SELECT
                * REPLACE(
                    CASE
                        WHEN {target} IS NULL
                        THEN {target}_previous
                            + ({target}_next - {target}_previous)
                            * ({time} - {time}_previous)::DOUBLE
                            / ({time}_next - {time}_previous)::DOUBLE 
                        ELSE {target}
                    END AS {target}
                )
            FROM {table}_bounds 
            """
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
            FROM read_csv('{Path(PATH_PACKAGE, "_input", f"deduplication_{level.name.lower()}.csv")}')
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
