import pytest
from unittest.mock import AsyncMock
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, clear_mappers

from DB_interface import Base


### фикструты для main.py
@pytest.fixture
def mock_session():
    return AsyncMock(spec=ClientSession)


@pytest.fixture
def setup_mock_response(mock_session):
    def _setup(status: int = 200):
        mock_response = AsyncMock()
        mock_response.status = status
        mock_response.content.read = AsyncMock(return_value=b"")
        mock_session.get.return_value.__aenter__.return_value = mock_response
        return mock_response
    return _setup


### Фикстуры для теста базы данных
@pytest.fixture(scope="function")
async def test_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    yield async_session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()
    clear_mappers()