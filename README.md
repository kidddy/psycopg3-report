# Обзор Psycopg3

## Презентация

Для просмотра презентации нужна утилита [slides](https://github.com/maaslalani/slides). *Хотя можно просто прочитать `report.md`, ведь это обычный файл с markdown текстом.*

```bash
$ slides report.md
```

- `j` перейти к следущему слайду
- `k` перейти к предыдущему слайду
- `Esc` закрыть презентацию

## Бенчмарк

Здесь чутка переработанный бенчмарк от MagicStack ([ссылка на оригинал](https://github.com/MagicStack/pgbench)).

Для запуска вам понадобятся

- `Python3.7` *(не забудьте подготовить виртуальное окружение)*
- `docker` и `docker-compose`

```bash
$ pip install --upgrade pip==23.1.2 pip-tools
$ pip-sync
$ docker-compose up -d
```

Добавьте в переменные окружения. *Например через [direnv](https://github.com/direnv/direnv).*

```
export PYTHONPATH="./src"

export DB_USERNAME="test"
export DB_PASSWORD="test"
export DB_NAME="test"
export DB_HOST="localhost"
export DB_PORT=5432
```

а далее запускайте скрипты какие хотите =)

- `sync_example.py`, `async_example.py` - примеры-песочницы для работы с psycopg.
- `config.py` - прячет конфиги для подключения к базе
- `benchmark.py` - запустит все тесты из папки *queries* и результаты запишет в `summary_bench_result.json`. В принцепе можно распарсить этот json и смотреть результаты
- `show_results.py` - прочитает `summary_bench_result.json`, посчитает перцентили и напишет в stdin markdown таблицы. *из-за моих малых знаний numpy и лени, считается очень долго.*

*На докладе упоминал о костыле для asyncpg. Вот [ссылочка](https://github.com/sqlalchemy/sqlalchemy/issues/6467#issuecomment-864943824) на issue в котором поднялась эта тема.*
