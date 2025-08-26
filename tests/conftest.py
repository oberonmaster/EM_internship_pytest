import pytest
from unittest.mock import AsyncMock, patch, mock_open, MagicMock
from aiohttp import ClientSession


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