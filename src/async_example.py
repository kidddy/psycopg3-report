import asyncio

import psycopg
import sqlalchemy as sa
import sqlalchemy.schema
from sqlalchemy.ext.asyncio import create_async_engine

from config import db_settings


async def raw_psycopg():
    table_name = f"{raw_psycopg.__name__}_async_test"
    aconn = await psycopg.AsyncConnection.connect(f"postgresql://{db_settings.connection_string}")

    async with aconn as psycopg_async_connection:
        async with psycopg_async_connection.cursor() as async_cursor:
            await async_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            await async_cursor.execute(f"""
                CREATE TABLE {table_name} (
                    id serial PRIMARY KEY,
                    num integer,
                    data text)
                """)

            await async_cursor.executemany(
                f"INSERT INTO {table_name} (num, data) VALUES (%s, %s)",
                [
                    (100500, "raw psycopg async driver"),
                    (100600, "123'654"),
                ],
            )

            execution_result = await async_cursor.execute(f"SELECT * FROM {table_name}")

            result = [row async for row in execution_result]
            # result = [row async for row in async_cursor]
            # result = [await async_cursor.fetchone()]
            # result = await async_cursor.fetchall()

            for record in result:
                print(record)

            await psycopg_async_connection.commit()


sa_metadata = sa.MetaData()
table = sa.Table(
    "sqlalchemy_example_async_test", sa_metadata,
    sa.Column("id", sa.INTEGER, primary_key=True, autoincrement=True),
    sa.Column("num", sa.INTEGER),
    sa.Column("data", sa.VARCHAR)
)


async def sqlalchemy_example():
    engine = create_async_engine(f"postgresql+psycopg://{db_settings.connection_string}")
    async with engine.connect() as sa_async_connection:
        await sa_async_connection.execute(sa.schema.DropTable(table, if_exists=True))
        await sa_async_connection.execute(sa.schema.CreateTable(table, if_not_exists=True))

        await sa_async_connection.execute(
            sa.insert(table),
            [
                {"num": 100500, "data": "under sqlalchemy with async"},
                {"num": 100600, "data": "123'654"},
            ],
        )

        cursor_result = await sa_async_connection.execute(sa.select(table))

        for row in cursor_result.fetchall():
            print(row)

        await sa_async_connection.commit()


if __name__ == "__main__":
    func = raw_psycopg
    # func = sqlalchemy_example

    asyncio.run(func())
