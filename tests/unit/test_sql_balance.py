import duckdb
import pytest
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from iep.utils import balance


@pytest.fixture
def connection() -> DuckDBPyConnection:
    return duckdb.connect()


def test_missing_year(
    connection: DuckDBPyConnection,
) -> None:
    panel: DuckDBPyRelation = connection.sql("""
        SELECT * FROM (VALUES
            ('A', 2020, 1.0),
            ('A', 2022, 2.0)
        ) AS t(id, year, value)
    """)
    result: DuckDBPyRelation = balance(panel, "year", ["id"])

    assert result.shape == (3, 3)
    assert (
        result.filter("year = 2021").aggregate("bool_and(value IS NULL)").fetchone()[0]  # ty:ignore[not-subscriptable]
    )


def test_across_groups(connection: DuckDBPyConnection) -> None:
    panel: DuckDBPyRelation = connection.sql("""
        SELECT * FROM (VALUES
            ('A', 2020, 1.0),
            ('B', 2022, 2.0)
        ) AS t(id, year, value)
    """)
    result: DuckDBPyRelation = balance(panel, "year", ["id"])

    assert result.shape == (6, 3)
    years: set[int] = {r[0] for r in result.select("year").distinct().fetchall()}
    assert years == {2020, 2021, 2022}


def test_idempotent(connection: DuckDBPyConnection) -> None:
    panel: DuckDBPyRelation = connection.sql("""
        SELECT * FROM (VALUES
            ('A', 2020, 1.0), ('A', 2021, 2.0),
            ('B', 2020, 3.0), ('B', 2021, 4.0)
        ) AS t(id, year, value)
    """)
    result: DuckDBPyRelation = balance(panel, "year", ["id"])

    assert result.shape == (4, 3)
    assert result.aggregate("bool_and(value IS NOT NULL)").fetchone()[0]  # ty:ignore[not-subscriptable]


def test_single_row(connection: DuckDBPyConnection) -> None:
    panel: DuckDBPyRelation = connection.sql(
        "SELECT 'X' AS id, 2020 AS year, 1.0 AS value"
    )
    result: DuckDBPyRelation = balance(panel, "year", ["id"])

    assert result.shape == (1, 3)


def test_multiple_groups(connection: DuckDBPyConnection) -> None:
    panel: DuckDBPyRelation = connection.sql("""
        SELECT * FROM (VALUES
            ('US', 'NY', 2020, 1.0),
            ('US', 'CA', 2021, 2.0),
            ('UK', 'LN', 2020, 3.0)
        ) AS t(country, city, year, value)
    """)
    result: DuckDBPyRelation = balance(panel, "year", ["country", "city"])

    assert result.shape == (6, 4)
