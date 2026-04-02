from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum, auto


class Tables(StrEnum):
    iep_identifiers = auto()
    iep_pollutant_release = auto()
    iep_energy_inputs = auto()
    iep_emissions_to_air = auto()
    ipcc_emission_factors = auto()
    ukprtr = auto()
    frprtr = auto()


@dataclass(kw_only=True, frozen=True, slots=True)
class Cte:
    name: str
    query: str

    def to_sql(self) -> str:
        return f"{self.name} AS ({self.query})"


@dataclass(kw_only=True, frozen=True, slots=True)
class CteChain:
    ctes: tuple[Cte, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        duplicates = {
            cte_name
            for cte_name, count in Counter(cte.name for cte in self.ctes).items()
            if count > 1
        }
        if duplicates:
            raise ValueError(f"Duplicate CTE names: {duplicates}")

    def extend(self, other: "Cte | CteChain") -> "CteChain":
        if isinstance(other, CteChain):
            ctes = other.ctes
        elif isinstance(other, Cte):
            ctes = (other,)
        else:
            raise TypeError()
        return CteChain(ctes=self.ctes + ctes)

    @property
    def n(self) -> int:
        return len(self.ctes)

    @property
    def final(self) -> str:
        return self.ctes[-1].name

    def to_sql(self, recursive: bool = False) -> str:
        keyword = "WITH RECURSIVE" if recursive else "WITH"
        return f"{keyword} {', '.join(cte.to_sql() for cte in self.ctes)} SELECT * FROM {self.final}"


def balance(
    alias: str,
    time: str,
    groups: list[str],
    filter: str | None = None,
) -> CteChain:
    columns = [time] + groups
    columns_sql = ", ".join(f'"{c}"' for c in columns)
    groups_sql = ", ".join(f'"{c}"' for c in groups)
    exclude_sql = ", ".join(f'"{c}"' for c in columns)
    where_clause = f"WHERE {filter}" if filter else ""
    return CteChain(
        ctes=(
            Cte(
                name="year_range",
                query=f"""SELECT
                    {groups_sql},
                    MIN("{time}") AS _min_time,
                    MAX("{time}") AS _max_time
                FROM {alias}
                {where_clause}
                GROUP BY {groups_sql}""",
            ),
            Cte(
                name="full_panel",
                query=f"""SELECT time."{time}", year_range.* EXCLUDE (_min_time, _max_time)
                    FROM (SELECT DISTINCT "{time}" FROM {alias}) time 
                    CROSS JOIN year_range""",
            ),
            Cte(
                name="balanced",
                query=f"""SELECT full_panel.*, t.* EXCLUDE ({exclude_sql})
                    FROM full_panel 
                    LEFT JOIN {alias} t USING ({columns_sql})
                    JOIN year_range USING ({groups_sql})
                    WHERE full_panel."{time}" BETWEEN year_range._min_time AND year_range._max_time""",
            ),
        )
    )
