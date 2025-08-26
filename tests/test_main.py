import asyncio
import runpy

import pytest
import datetime
from unittest.mock import AsyncMock, patch, mock_open, MagicMock, call
from aiohttp import ClientSession

import main
from main import download_files


# Тест статус-кода
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url, status",
    [
        ("https://spimex.com/upload/reports/oil_xls/oil_xls_20250801162000.xls", 200),
        ("https://spimex.com/upload/reports/oil_xls/not_found.xls", 404),
    ]
)
async def test_download_files_status(mock_session, setup_mock_response, url, status):
    mock_response = setup_mock_response(status)

    await download_files(mock_session, url)

    mock_session.get.assert_called_once_with(url)

    assert mock_response.status == status


# Тест правильности имени файла
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url, expected_filename",
    [
        ("https://example.com/file1.txt", "file1.txt"),
        ("https://example.com/path/to/file2.csv", "file2.csv"),
        ("https://example.com/file3.xls?query=123", "file3.xls?query=123"),
    ]
)
async def test_filename_creation(mock_session, setup_mock_response, url, expected_filename):

    setup_mock_response(200)

    m_open = mock_open()
    with patch("builtins.open", m_open):
        await download_files(mock_session, url)

    m_open.assert_called_with(expected_filename, "wb")
    mock_session.get.assert_called_once_with(url)

# Тест семафора
@pytest.mark.asyncio
async def test_semaphore_limit(mock_session):

    active = 0
    max_active = 0

    original_download = download_files

    async def download_wrapper(session, url):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await original_download(session, url)
        active -= 1

    urls = [f"https://example.com/file{i}.txt" for i in range(10)]
    await asyncio.gather(*(download_wrapper(mock_session, url) for url in urls))

    assert max_active <= 5

# Строка 30 файла main.py
@pytest.mark.asyncio
async def test_download_file_chunk(mock_session, setup_mock_response):
    url = "https://spimex.com/upload/reports/oil_xls/oil_xls_20250801162000.xls"

    mock_response = setup_mock_response(status=200)
    mock_response.content.read = AsyncMock(side_effect=[b"abc", b""])

    mock_session.get.return_value.__aenter__.return_value = mock_response

    m = mock_open()
    with patch("builtins.open", m):
        await download_files(mock_session, url)

    handle = m()
    handle.write.assert_called_once_with(b"abc")


# Строки 34-35 файла main.py
@pytest.mark.asyncio
async def test_download_file_exception(mock_session):
    url = "https://spimex.com/upload/reports/oil_xls/oil_xls_20250801162000.xls"

    mock_session.get.side_effect = Exception("Test error")

    with patch("builtins.print") as mock_print:
        await download_files(mock_session, url)

    mock_print.assert_called_once_with(f"Ошибка при загрузке {url}: Test error")


# Строки 40-60 файла main.py
@pytest.mark.asyncio
async def test_main(monkeypatch):
    # 1) Зафиксируем время: end_date = 2025-07-03
    class FixedDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 7, 3, 10, 0, 0)  # любое время в этот день

    # В main импортирован модуль `datetime`, меняем у него класс datetime
    monkeypatch.setattr(main.datetime, "datetime", FixedDatetime)

    # 2) Замокаем зависимости
    mock_create_tables = AsyncMock()
    mock_download_files = AsyncMock()
    mock_parse_to_db = AsyncMock()

    monkeypatch.setattr(main, "create_tables", mock_create_tables)
    monkeypatch.setattr(main, "download_files", mock_download_files)
    monkeypatch.setattr(main, "parse_to_db", mock_parse_to_db)

    # 3) Подменим aiohttp.ClientSession на простой асинхронный КМ
    class DummySessionCM:
        async def __aenter__(self):
            return "SESSION"
        async def __aexit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(main.aiohttp, "ClientSession", lambda: DummySessionCM())

    # 4) Чтобы после загрузки что-то распарсилось, заранее положим имена файлов
    main.filenames.clear()
    main.filenames.extend([
        "oil_xls_20250701162000.xls",
        "oil_xls_20250702162000.xls",
        "oil_xls_20250703162000.xls",
    ])

    # 5) Запускаем main()
    await main.main()

    # 6) Проверки
    # create_tables вызван
    mock_create_tables.assert_awaited_once()

    # сгенерированные URL-ы соответствуют датам 01..03 июля 2025
    expected_dates = ["20250701", "20250702", "20250703"]
    expected_urls = [
        f"https://spimex.com/upload/reports/oil_xls/oil_xls_{d}162000.xls"
        for d in expected_dates
    ]

    # download_files вызван по одному разу на каждый URL, и первый аргумент — это "SESSION"
    calls = mock_download_files.await_args_list
    called_urls = [c.args[1] for c in calls]
    assert called_urls == expected_urls
    assert all(c.args[0] == "SESSION" for c in calls)

    # parse_to_db вызван для каждого файла из глобального списка
    mock_parse_to_db.assert_has_awaits(
        [call(name) for name in main.filenames],
        any_order=True
    )

# Строка 64 файла main.py
def test_entrypoint_runs_main(monkeypatch):
    called = {}

    def fake_run(coro):
        called["coro"] = coro
        coro.close()

    monkeypatch.setattr(main.asyncio, "run", fake_run)

    runpy.run_module("main", run_name="__main__")

    assert called["coro"].__name__ == "main"