"""Local conftest: the parent `tests/conftest.py` autouse-creates every
table in `Base.metadata` against SQLite, but `portfolio_daily_snapshots`
declares a Postgres-only JSONB column that the SQLite dialect can't
render — so the whole test session crashes at startup before any test
in this module runs.

The tests in this subdir don't touch the DB at all (they assert on
AsyncMock call args), so we override `setup_database` to a no-op
fixture. Once the model is portable (JSON.with_variant(JSONB, "postgresql"))
or we move integration tests to a Postgres test container, this
override can be deleted.
"""
import pytest_asyncio


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_database():
    yield
