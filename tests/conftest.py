import pytest
from unittest.mock import AsyncMock, patch
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


# фикстуры для эндпоинтов
@pytest.fixture
def mock_trading_result():
    """Фикстура с примером результата торгов"""
    return {
        "id": 1,
        "exchange_product_id": "A100",
        "exchange_product_name": "Нефть сырая",
        "oil_id": "RU000A0JX0J2",
        "delivery_basis_id": "BASIS001",
        "delivery_basis_name": "FOB",
        "delivery_type_id": "TYPE001",
        "volume": 1000,
        "total": 500000,
        "count": 50,
        "date": "2025-07-01"
    }


@pytest.fixture
def mock_trading_results(mock_trading_result):
    """Фикстура со списком результатов торгов"""
    return [mock_trading_result]


@pytest.fixture
def mock_ural_trading_result():
    """Фикстура с результатом торгов для URAL"""
    return {
        "id": 1,
        "exchange_product_id": "A100",
        "exchange_product_name": "Нефть сырая",
        "oil_id": "URAL",
        "delivery_basis_id": "BAS",
        "delivery_basis_name": "FOB",
        "delivery_type_id": "T",
        "volume": 1000,
        "total": 500000,
        "count": 50,
        "date": "2025-07-01"
    }


@pytest.fixture
def mock_cached_dates():
    """Фикстура с кэшированными датами"""
    return ["2025-07-03", "2025-07-02", "2025-07-01"]


# Общие моки для патчей
@pytest.fixture(autouse=True)
def common_mocks():
    """Общие моки для всех тестов"""
    with patch('app.cache_invalidation_dep'), \
            patch('app.get_cache', return_value=None), \
            patch('app.set_cache'):
        yield
