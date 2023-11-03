---
author: abrailov@astralinux.ru
---
# psycopg3

Новый драйвер для Postgres в Python.

- Что нового?
- Сравнение с `asyncpg`
- Встраиваем в **acm-configuration-service**

---

## Прямое использование
### Cинхронный стиль (часть 1)

```python
with psycopg.connect(f"postgresql://test:test@localhost:5432/test-db") as psycopg_connection:
    table_name = f"table_test"
    # Откроем курсор для взаимодействия с базой
    with psycopg_connection.cursor() as cursor:
        # Создадим таблицу
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id serial PRIMARY KEY,
                num integer,
                data text)
        """)

        # Закинем в таблицу несколько значений
        cursor.executemany(
            f"INSERT INTO {table_name} (num, data) VALUES (%s, %s)",
            [
                (100500, "raw psycopg driver"),
                (100600, "123'654"),
            ],
        )
```

---

## Прямое использование
### Cинхронный стиль (часть 2)

```python
# А теперь достанем эти значения и напечатаем на экран
execution_result = cursor.execute(f"SELECT * FROM {table_name}")

# Есть много способов извлечь данные из курсора
result = execution_result
# result = cursor.fetchall()
# result = [ cursor.fetchone() ]
# result = [*cursor]
for record in result:
    print(record)

# сделаем коммит
psycopg_connection.commit()
```

---

## Прямое использование
### Ассинхронный стиль (часть 1)

```python
# Устанавливаем соединение с базой
aconn = await psycopg.AsyncConnection.connect(f"postgresql://{db_settings.connection_string}")

# Используем контекстный менеджер, чтобы коннект был закрыт в конце
async with aconn as psycopg_async_connection:
    table_name = f"table_async_test"

    # Откроем курсор для взаимодействия с базой
    async with psycopg_async_connection.cursor() as async_cursor:
        # Создадим таблицу
        await async_cursor.execute(f"""
            CREATE TABLE {table_name} (
                id serial PRIMARY KEY,
                num integer,
                data text)
            """)

        # Закинем в таблицу несколько значений
        await async_cursor.executemany(
            f"INSERT INTO {table_name} (num, data) VALUES (%s, %s)",
            [
                (100500, "raw psycopg async driver"),
                (100600, "123'654"),
            ],
        )
```

---

## Прямое использование
### Ассинхронный стиль (часть 2)

```python
# А теперь достанем эти значения и напечатаем на экран
execution_result = await async_cursor.execute(f"SELECT * FROM {table_name}")

# Есть много способов извлечь данные из курсора
result = [row async for row in execution_result]
# result = [row async for row in async_cursor]
# result = [await async_cursor.fetchone()]
# result = await async_cursor.fetchall()

for record in result:
    print(record)

# сделаем коммит
await psycopg_async_connection.commit()
```

---

## Использование в алхимии
### Синхронный стиль

```python
sa_metadata = sa.MetaData()
table = sa.Table(
    "sqlalchemy_example_async_test", sa_metadata,
    sa.Column("id", sa.INTEGER, primary_key=True, autoincrement=True),
    sa.Column("num", sa.INTEGER),
    sa.Column("data", sa.VARCHAR)
)

def sqlalchemy_example():
    engine = sa.create_engine(f"postgresql+psycopg://{db_settings.connection_string}")
    with engine.connect() as sa_connection:
        table.create(bind=sa_connection)

        sa_connection.execute(
            sa.insert(table),
            [
                {"num": 100500, "data": "under sqlalchemy"},
                {"num": 100600, "data": "123'654"},
            ]
        )

        result = sa_connection.execute(sa.select(table)).fetchall()
        for row in result:
            print(row)

        sa_connection.commit()
```

---

## Использование в алхимии
### Ассинхронный стиль

```python
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
```

---

## Заявленные фичи

---

## Заявленные фичи

- Поддержка asyncio

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона

```python

# Записать в базу
records = [(10, 20, "hello"), (40, None, "world")]

with cursor.copy("COPY sample (col1, col2, col3) FROM STDIN") as copy:
    for record in records:
        copy.write_row(record)
```

```python
# Скопировать из базы
with cur.copy("COPY (VALUES (10::int, current_date)) TO STDOUT") as copy:
    for row in copy.rows():
        print(row)  # return unparsed data: ('10', '2046-12-24')
```

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool

```python
# Пример синхронный
with ConnectionPool(...) as pool:
    with pool.connection() as conn:
        conn.execute("SELECT something FROM somewhere ...")

        with conn.cursor() as cur:
            cur.execute("SELECT something else...")

    # В конце контекста `connection()`, транзакция коммитится
    # или отктывается, а коннект возвращается в pool

# В конце контекста pool, все ресурсы используемые пулом освобождаются
```

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool

```python
# Пример ассинхронный
async with AsyncConnectionPool(...) as pool:
    async with pool.connection() as conn:
        await conn.execute("SELECT something FROM somewhere ...")

        with conn.cursor() as cur:
            await cur.execute("SELECT something else...")

    # В конце контекста `connection()`, транзакция коммитится
    # или отктывается, а коннект возвращается в pool

# В конце контекста pool, все ресурсы используемые пулом освобождаются
```

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool

```python
# Как еще работать с пулом?
pool = ConnectionPool(..., open=False, ...)

@app.on_event("startup")
def open_pool():
    pool.open()
    app.state.db_pool = pool

@app.on_event("shutdown")
def close_pool():
    pool.close()
    del app.state.db_pool
```

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool
- Поддержка статической типизации

```python
conn = psycopg.connect() # type is psycopg.Connection[tuple[Any, ...]]
cur = conn.cursor()      # type is psycopg.Cursor[tuple[Any, ...]]
rec = cur.fetchone()     # type is Optional[tuple[Any, ...]]
recs = cur.fetchall()    # type is List[tuple[Any, ...]]
```

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool
- Поддержка статической типизации
- Связывание параметров с запросами на сервере

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool
- Поддержка статической типизации
- Связывание параметров с запросами на сервере
- Prepared statements (заготовленные запросы)

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool
- Поддержка статической типизации
- Связывание параметров с запросами на сервере
- Prepared statements (заготовленные запросы)

Запрос автоматически становится заготовленным, когда выполнится более `Connection.preare_threshold` раз. Можно указать `None`, чтобы отключить заготовки.

Можно указать `conn.execute(some_query, preare=False)`, чтобы отключить заготовку.

**!!** Тут важно помнить, что *prepared-statements* не совместимы с **pg_bouncer**.

---

## Заявленные фичи

- Поддержка asyncio
- Поддержка postgres COPY из питона
- Переработанный Сonnection Pool
- Поддержка статической типизации
- Связывание параметров с запросами на сервере
- Prepared statements (заготовленные запросы)

И остальные, в которых я не разобрался

- Statements pipeline
- Binary communication
- Direct access to the libpq functionalities

Ссылка на все фичи: https://www.psycopg.org/psycopg3/docs/index.html

---

## Отличия от `asyncpg`
### Как оно было раньше?

`asyncpg` на фоне `psycopg2` выделяется фичами:
- бинарный I/O протокол
- prepared-statements
- не надо ставить `libpq`. `asyncpg` нативно реализовали взаимодействие с `postgres`.

Зато `asyncpg` не соответствует `PEP-249` **Python Database API Specification v2.0**

https://peps.python.org/pep-0249

---

## Отличия от `asyncpg`

Раньше было строго понятно: хочешь использовать `asyncio` для взаимодействия с базой - ставь в зависимости `asyncpg`. Иначе бери `psycopg2`.

**ну еще есть `aiopg`, но функционал в нем урезан, поэтому не буду сегодня про него.*

А вот как сейчас понять что лучше использовать? По набору функционала они примерно одинаковы.

---

## Отличия от `asyncpg`

Раньше было строго понятно: хочешь использовать `asyncio` для взаимодействия с базой - ставь в зависимости `asyncpg`. Иначе бери `psycopg2`.

**ну еще есть `aiopg`, но функционал в нем урезан, поэтому не буду сегодня про него.*

А вот как сейчас понять что лучше использовать? По набору функционала они примерно одинаковы.

### Сравним по количеству вопросов на Stackoverflow

- `asyncpg` - 500 вопросов
- `psycopg3` - 185 вопросов

---

## Бенчмарк
### 1-pg_type.json

- 10 конкурентных процессов
- 30 секунд

```sql
SELECT typname, typnamespace, typowner, typlen, typbyval, typcategory,
       typispreferred, typisdefined, typdelim, typrelid, typelem, typarray 
    FROM pg_type WHERE typtypmod = $1 AND typisdefined = $2
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 55510 | 33916610 | 30.0 | 387 | 3772 | 499.0 | 534.0 |
| psycopg | 22313 | 13633243 | 30.01 | 297 | 3330 | 1311.0 | 1817.0 |
| psycopg_async | 21192 | 12948312 | 30.01 | 588 | 2643 | 1419.0 | 1563.0 |

---

## Бенчмарк
### 2-generate_series.json

- 10 конкурентных процессов
- 30 секунд

```sql
SELECT i FROM generate_series(1, $1) AS i
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 117830 | 117830000 | 30.0 | 132 | 1751 | 197.0 | 243.0 |
| psycopg | 36847 | 36847000 | 30.01 | 150 | 4340 | 731.0 | 1304.0 |
| psycopg_async | 35338 | 35338000 | 30.01 | 340 | 2910 | 707.0 | 1410.0 |

---

## Бенчмарк
### 3-large_object.json

- 10 конкурентных процессов
- 30 секунд

```sql
CREATE TABLE _bytes(b bytea);
INSERT INTO _bytes(b) (SELECT repeat('a', 1000)::bytea FROM generate_series(1, 100));

SELECT * FROM _bytes; // Повторяется в течение 30 секунд

DROP TABLE _bytes;
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 240225 | 24022500 | 30.0 | 79 | 464 | 124.0 | 134.0 |
| psycopg | 38062 | 3806200 | 30.01 | 155 | 1727 | 781.0 | 1011.0 |
| psycopg_async | 30593 | 3059300 | 30.01 | 651 | 1513 | 974.0 | 1040.8 |

---

## Бенчмарк
### 4-arrays.json

- 10 конкурентных процессов
- 30 секунд

```sql
CREATE TABLE _test(a int[]);
INSERT INTO _test(a) (
    SELECT (
        SELECT array_agg(i) FROM generate_series(1, 100) as s(i)
    ) FROM generate_series(1, 100)
);

SELECT * FROM _test;  // Повторяется в течение 30 секунд

DROP TABLE _test;
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 76258 | 7625800 | 30.0 | 302 | 736 | 395.0 | 422.0 |
| psycopg | 28295 | 2829500 | 30.01 | 184 | 4874 | 999.0 | 1632.0 |
| psycopg_async | 29612 | 2961200 | 30.01 | 359 | 3948 | 964.0 | 1333.0 |

---

## Бенчмарк
### 5-copyfrom.json

- 10 конкурентных процессов
- 30 секунд

```sql
CREATE TABLE _test(a int, b int, c int, d text, e float, f int, g text);

COPY _test(a, b, c, d, e, f, g) FROM STDIN;  // Повторяется в течение 30 секунд
// 1000 rows: [10, 11, 10, "TESTTESTTEST", 10.333, 12341234, "123412341234"]

DROP TABLE _test;
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 4573 | 45730000 | 30.01 | 1709 | 66735 | 5907.0 | 6722.8 |
| psycopg | 1267 | 12670000 | 30.18 | 6580 | 62304 | 23153.0 | 32612.0 |
| psycopg_async | 1280 | 12800000 | 30.03 | 3481 | 50078 | 23028.0 | 31303.6 |

---

## Бенчмарк
### 6-batch.json

- 10 конкурентных процессов
- 30 секунд

```sql
CREATE TABLE _test(a int, b int, c int, d text, e float, f int, g text);

INSERT INTO _test (a, b, c, d, e, f, g) VALUES ($1, $2, $3, $4, $5, $6, $7);
// 1000 rows: [10, 11, 10, "TESTTESTTEST", 10.333, 12341234, "123412341234"]

DROP TABLE _test;
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 9008 | 9008000 | 30.01 | 1306 | 14397 | 3297.0 | 3652.3 |
| psycopg | 350 | 350000 | 30.28 | 58526 | 93191 | 86391.5 | 88950.1 |
| psycopg_async | 330 | 330000 | 30.21 | 88993 | 95630 | 91405.5 | 93635.2 |

---

## Бенчмарк
### 7-oneplusone.json

- 10 конкурентных процессов
- 30 секунд

```sql
SELECT 1+1;
```

| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |
|--|--|--|--|--|--|--|--|
| asyncpg | 797499 | 797499 | 30.0 | 18 | 170 | 37.0 | 42.0 |
| psycopg | 230022 | 230022 | 30.0 | 16 | 664 | 118.0 | 216.0 |
| psycopg_async | 240166 | 240166 | 30.0 | 57 | 332 | 116.0 | 148.0 |

---
