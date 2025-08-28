from sqlalchemy import inspect
import pytest

import DB_interface as db


"""
Тестируем реальную БД(postgres)
Прндварительно нужно запустить выполнение main.py чтобы создалась таблица бд и заполнилась данными
"""


@pytest.mark.asyncio
async def test_create_tables_real_db():
    await db.create_tables()

    async with db.engine.begin() as conn:
        def check_tables(sync_conn):
            inspector = inspect(sync_conn)
            return inspector.get_table_names()

        tables = await conn.run_sync(check_tables)

    assert "spimex_trading_results" in tables


# строки 49 - 135



