from unittest.mock import patch, AsyncMock, MagicMock
from datetime import date
from fastapi.testclient import TestClient

from app import app

client = TestClient(app)

#TODO seconds_until_next_invalidation
#TODO invalidate_cache_if_needed
#TODO get_cache
#TODO set_cache
#TODO model_to_serializable
#TODO cache_invalidation_dep

# ---------- Endpoints ----------
def test_get_last_trading_results_status_and_structure(mock_trading_results):
    """Тест проверки статус кода и структуры данных с моками"""

    with patch('app.get_last_trading_date', new=AsyncMock(return_value="2025-07-01")), \
            patch('app.get_trading_results', new=AsyncMock(return_value=mock_trading_results)), \
            patch('app.model_to_serializable', side_effect=lambda x: x):
        response = client.get("/last_results")

        # Проверяем статус код
        assert response.status_code == 200

        # Проверяем, что ответ в формате JSON
        assert response.headers["content-type"] == "application/json"

        # Парсим JSON
        data = response.json()

        # Проверяем, что это список
        assert isinstance(data, list)
        assert len(data) == 1

        # Проверяем структуру данных
        first_item = data[0]

        # Проверяем обязательные поля
        required_fields = [
            "id", "exchange_product_id", "exchange_product_name", "oil_id",
            "delivery_basis_id", "delivery_basis_name", "delivery_type_id",
            "volume", "total", "count", "date"
        ]

        for field in required_fields:
            assert field in first_item


def test_get_last_trading_results_cached(mock_trading_results):
    """Тест получения данных из кэша"""

    with patch('app.get_cache', return_value=mock_trading_results), \
            patch('app.get_last_trading_date', new=AsyncMock()), \
            patch('app.get_trading_results', new=AsyncMock()):
        response = client.get("/last_results")

        assert response.status_code == 200
        assert response.json() == mock_trading_results


def test_get_last_trading_results_no_data():
    """Тест случая, когда нет данных"""

    with patch('app.get_cache', return_value=None), \
            patch('app.get_last_trading_date', new=AsyncMock(return_value=None)):
        response = client.get("/last_results")

        assert response.status_code == 404
        assert response.json()["detail"] == "No trading data found"


def test_get_dynamics_status_and_structure(mock_trading_results):
    """Тест проверки статус кода и структуры данных для эндпоинта dynamics"""

    with patch('app.get_dynamics', new=AsyncMock(return_value=mock_trading_results)), \
            patch('app.model_to_serializable', side_effect=lambda x: x):
        response = client.get("/dynamics?start_date=2025-07-01&end_date=2025-07-03")

        # Проверяем статус код
        assert response.status_code == 200

        # Проверяем, что ответ в формате JSON
        assert response.headers["content-type"] == "application/json"

        # Парсим JSON
        data = response.json()

        # Проверяем, что это список
        assert isinstance(data, list)
        assert len(data) == 1

        # Проверяем обязательные поля
        first_item = data[0]
        required_fields = [
            "id", "exchange_product_id", "exchange_product_name", "oil_id",
            "delivery_basis_id", "delivery_basis_name", "delivery_type_id",
            "volume", "total", "count", "date"
        ]

        for field in required_fields:
            assert field in first_item


def test_get_dynamics_with_filters():
    """Тест с опциональными фильтрами"""

    with patch('app.get_dynamics', new=AsyncMock(return_value=[])), \
            patch('app.model_to_serializable', side_effect=lambda x: x):
        response = client.get(
            "/dynamics?start_date=2025-07-01&end_date=2025-07-03"
            "&oil_id=test_oil&delivery_type_id=test_type&delivery_basis_id=test_basis&limit=100"
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        assert isinstance(response.json(), list)


def test_get_dynamics_invalid_dates():
    """Тест с некорректными датами (start_date > end_date)"""

    response = client.get("/dynamics?start_date=2025-07-03&end_date=2025-07-01")

    assert response.status_code == 400
    assert response.json()["detail"] == "start_date must be <= end_date"


def test_get_dynamics_cached(mock_trading_results):
    """Тест получения dynamics из кэша"""

    with patch('app.get_cache', return_value=mock_trading_results), \
            patch('app.get_dynamics', new=AsyncMock()):
        response = client.get("/dynamics?start_date=2025-07-01&end_date=2025-07-03")

        assert response.status_code == 200
        assert response.json() == mock_trading_results


def test_get_last_trading_dates(mock_cached_dates):
    """Тест для эндпоинта /last_dates"""

    with patch('app.get_cache', return_value=None), \
            patch('app.async_session') as mock_async_session:
        # Мокируем асинхронную сессию
        mock_session = AsyncMock()
        mock_async_session.return_value.__aenter__.return_value = mock_session

        # Мокируем результат запроса
        mock_result = MagicMock()
        mock_result.all.return_value = [(date(2025, 7, 3),), (date(2025, 7, 2),), (date(2025, 7, 1),)]
        mock_session.execute.return_value = mock_result

        response = client.get("/last_dates?limit=3")

        # Проверяем статус код
        assert response.status_code == 200

        # Проверяем, что ответ в формате JSON
        assert response.headers["content-type"] == "application/json"

        # Проверяем данные
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert all(isinstance(d, str) for d in data)  # Даты в формате строк


def test_get_last_trading_dates_cached(mock_cached_dates):
    """Тест для эндпоинта /last_dates с кэшированными данными"""

    with patch('app.get_cache', return_value=mock_cached_dates), \
            patch('app.async_session'):
        response = client.get("/last_dates?limit=3")

        assert response.status_code == 200
        data = response.json()
        assert data == mock_cached_dates


def test_get_last_trading_dates_default_limit():
    """Тест для эндпоинта /last_dates с дефолтным лимитом"""

    with patch('app.get_cache', return_value=None), \
            patch('app.async_session') as mock_async_session:
        mock_session = AsyncMock()
        mock_async_session.return_value.__aenter__.return_value = mock_session

        mock_result = MagicMock()
        mock_result.all.return_value = [(date(2025, 7, 1),)]
        mock_session.execute.return_value = mock_result

        response = client.get("/last_dates")

        assert response.status_code == 200
        # Проверяем, что был вызван запрос с лимитом 10 (по умолчанию)
        mock_session.execute.assert_called_once()


def test_api_get_trading_results(mock_ural_trading_result):
    """Тест для эндпоинта /results"""

    mock_results = [mock_ural_trading_result]

    with patch('app.get_trading_results', new=AsyncMock(return_value=mock_results)), \
            patch('app.model_to_serializable', side_effect=lambda x: x):
        response = client.get("/results?oil_id=URAL&limit=100")

        # Проверяем статус код
        assert response.status_code == 200

        # Проверяем, что ответ в формате JSON
        assert response.headers["content-type"] == "application/json"

        # Проверяем данные
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["oil_id"] == "URAL"


def test_api_get_trading_results_with_all_filters():
    """Тест для эндпоинта /results со всеми фильтрами"""

    with patch('app.get_trading_results', new=AsyncMock(return_value=[])), \
            patch('app.model_to_serializable', side_effect=lambda x: x):
        response = client.get(
            "/results?oil_id=URAL&delivery_type_id=T&delivery_basis_id=BAS&date_value=2025-07-01&limit=50"
        )

        assert response.status_code == 200
        data = response.json()
        assert data == []


def test_api_get_trading_results_cached(mock_ural_trading_result):
    """Тест для эндпоинта /results с кэшированными данными"""

    cached_data = [mock_ural_trading_result]

    with patch('app.get_cache', return_value=cached_data), \
            patch('app.get_trading_results', new=AsyncMock()) as mock_get_trading:
        response = client.get("/results?oil_id=URAL&limit=100")

        assert response.status_code == 200
        assert response.json() == cached_data
        # Не должен вызываться при наличии кэша
        mock_get_trading.assert_not_called()


def test_api_get_trading_results_default_limit():
    """Тест для эндпоинта /results с дефолтным лимитом"""

    with patch('app.get_trading_results', new=AsyncMock(return_value=[])) as mock_get_trading, \
            patch('app.model_to_serializable', side_effect=lambda x: x):
        response = client.get("/results")

        assert response.status_code == 200
        # Проверяем, что был вызван с лимитом 100 (по умолчанию)
        mock_get_trading.assert_called_once_with(
            limit=100, oil_id=None, delivery_type_id=None,
            delivery_basis_id=None, date_value=None
        )
