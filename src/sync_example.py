import psycopg
import sqlalchemy as sa

from config import db_settings


def raw_psycopg():
    with psycopg.connect(f"postgresql://{db_settings.connection_string}") as psycopg_connection:
        table_name = f"{raw_psycopg.__name__}_test"
        # Откроем курсор для взаимодействия с базой
        with psycopg_connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Создадим таблицу
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    id serial PRIMARY KEY,
                    num integer,
                    data text)
                """)

            # Pass data to fill a query placeholders and let Psycopg perform the correct conversion (no SQL injections!)
            # Закинем в таблицу несколько значений
            cursor.executemany(
                f"INSERT INTO {table_name} (num, data) VALUES (%s, %s)",
                [
                    (100500, "raw psycopg driver"),
                    (100600, "123'654"),
                ],
            )

            # Query the database and obtain data as Python objects.
            # А теперь достанем эти значения и напечатаем на экран
            execution_result = cursor.execute(f"SELECT * FROM {table_name}")  # will return [(1, 100, "abc'def")]

            # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
            # of several records, or even iterate on the cursor
            result = execution_result
            # result = cursor.fetchall()
            # result = [ cursor.fetchone() ]
            # result = [*cursor]
            for record in result:
                print(record)

            # Make the changes to the database persistent
            psycopg_connection.commit()


sa_metadata = sa.MetaData()
table = sa.Table(
    "sqlalchemy_example_test", sa_metadata,
    sa.Column("id", sa.INTEGER, primary_key=True, autoincrement=True),
    sa.Column("num", sa.INTEGER),
    sa.Column("data", sa.VARCHAR)
)


def sqlalchemy_example():
    engine = sa.create_engine(f"postgresql+psycopg://{db_settings.connection_string}")
    with engine.connect() as sa_connection:
        table.drop(bind=sa_connection, checkfirst=True)
        table.create(bind=sa_connection)

        sa_connection.execute(
            sa.insert(table),
            [
                {"num": 100500, "data": "under sqlalchemy"},
                {"num": 100600, "data": "123'654"},
            ],
        )

        result = sa_connection.execute(sa.select(table)).fetchall()
        for row in result:
            print(row)

        sa_connection.commit()


if __name__ == "__main__":
    raw_psycopg()
    # sqlalchemy_example()
