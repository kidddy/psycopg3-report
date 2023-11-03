#!/usr/bin/env python3
# original script: https://github.com/MagicStack/pgbench/blob/master/_python/pgbench_python.py


import argparse
import asyncio
import csv
import io
import json
import re
import sys
import time
from collections import defaultdict
from concurrent import futures
from pathlib import Path

import asyncpg
import numpy as np
import psycopg
import uvloop

from config import db_settings


class PsycoPG:
    arg_format = 'python'
    is_async = False

    @staticmethod
    def connect():
        conn = psycopg.connect(user=db_settings.USERNAME, host=db_settings.HOST,
                                port=db_settings.PORT, password=db_settings.PASSWORD)
        return conn

    @staticmethod
    def execute(conn, query, args):
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)
        cur.execute(query, args)
        return len(cur.fetchall())

    @staticmethod
    def executemany(conn, query, args):
        with conn.cursor() as cur:
            cur.executemany(query, args)
        return len(args)

    @staticmethod
    def copy(conn, query, args):
        rows, copy = args[:2]
        f = io.StringIO()
        writer = csv.writer(f, delimiter='\t')
        for row in rows:
            writer.writerow(row)
        f.seek(0)
        with conn.cursor() as cur:
            with cur.copy(query) as copy:
                copy.write(f.read())
            conn.commit()
            return cur.rowcount


class AsyncPG:
    arg_format = 'native'
    is_async = True

    @staticmethod
    async def connect():
        conn = await asyncpg.connect(user=db_settings.USERNAME, host=db_settings.HOST,
                                port=db_settings.PORT, password=db_settings.PASSWORD)
        return conn

    @staticmethod
    async def execute(conn, query, args):
        return len(await conn.fetch(query, *args))

    @staticmethod
    async def executemany(conn, query, args):
        await conn.executemany(query, args)
        return len(args)

    @staticmethod
    async def copy(conn, query, args):
        rows, copy = args[:2]
        result = await conn.copy_records_to_table(
            copy['table'], columns=copy['columns'], records=rows)
        cmd, _, count = result.rpartition(' ')
        return int(count)


class AsyncPsycoPG:
    arg_format = 'python'
    is_async = True

    @staticmethod
    async def connect():
        conn = await psycopg.AsyncConnection.connect(
            user=db_settings.USERNAME, host=db_settings.HOST,
            port=db_settings.PORT, password=db_settings.PASSWORD,
        )
        return conn

    @staticmethod
    async def execute(conn, query, args):
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)
        await cur.execute(query, args)
        return len(await cur.fetchall())

    @staticmethod
    async def executemany(conn, query, args):
        async with conn.cursor() as cur:
            await cur.executemany(query, args)
        return len(args)

    @staticmethod
    async def copy(conn, query, args):
        rows, copy = args[:2]
        f = io.StringIO()
        writer = csv.writer(f, delimiter='\t')
        for row in rows:
            writer.writerow(row)
        f.seek(0)

        async with conn.cursor() as cur:
            async with cur.copy(query) as copy:
                await copy.write(f.read())
            await conn.commit()
            return cur.rowcount


async def async_worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        rows += await executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1

    return queries, rows, latency_stats, min_latency, max_latency


def sync_worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        rows += executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1

    return queries, rows, latency_stats, min_latency, max_latency


async def runner(
    args,
    driver,
    query,
    query_args,
    setup,
    teardown,
):
    query_args = [*query_args]

    timeout = args.timeout * 1000

    if driver.arg_format == 'python':
        query = re.sub(r'\$\d+', '%s', query)

    is_copy = query.startswith('COPY ')
    is_batch = query_args and isinstance(query_args[0], dict)


    executor = driver.execute

    if is_copy:
        if driver.copy is None:
            raise RuntimeError('COPY is not supported for {}'.format(executor))
        executor = driver.copy

        match = re.match('COPY (\w+)\s*\(\s*((?:\w+)(?:,\s*\w+)*)\s*\)', query)
        if not match:
            raise RuntimeError('could not parse COPY query')

        query_info = query_args[0]
        query_args[0] = [query_info['row']] * query_info['count']
        query_args.append({
            'table': match.group(1),
            'columns': [col.strip() for col in match.group(2).split(',')]
        })
    elif is_batch:
        if driver.executemany is None:
            raise RuntimeError('batch is not supported for {}'.format(executor))
        executor = driver.executemany

        query_info = query_args[0]
        query_args = [query_info['row']] * query_info['count']

    conns = []

    for i in range(args.concurrency):
        if driver.is_async:
            conn = await driver.connect()
        else:
            conn = driver.connect()
        conns.append(conn)

    async def _do_run(run_duration):
        start = time.monotonic()

        tasks = []

        if driver.is_async:
            # Asyncio driver
            for i in range(args.concurrency):
                task = async_worker(executor, [conns[i], query, query_args],
                              start, run_duration, timeout)
                tasks.append(task)

            results = await asyncio.gather(*tasks)
        else:
            # Sync driver
            with futures.ThreadPoolExecutor(max_workers=args.concurrency) as e:
                for i in range(args.concurrency):
                    task = e.submit(sync_worker, executor,
                                    [conns[i], query, query_args],
                                    start, run_duration, timeout)
                    tasks.append(task)

                results = [fut.result() for fut in futures.wait(tasks).done]

        end = time.monotonic()

        return results, end - start

    if setup:
        admin_conn = await AsyncPG.connect()
        await admin_conn.execute(setup)

    try:
        try:
            if args.warmup_time:
                await _do_run(args.warmup_time)

            results, duration = await _do_run(args.duration)
        finally:
            for conn in conns:
                if driver.is_async:
                    await conn.close()
                else:
                    conn.close()

        min_latency = float('inf')
        max_latency = 0.0
        queries = 0
        rows = 0
        latency_stats = None

        for result in results:
            t_queries, t_rows, t_latency_stats, t_min_latency, t_max_latency =\
                result
            queries += t_queries
            rows += t_rows
            if latency_stats is None:
                latency_stats = t_latency_stats
            else:
                latency_stats = np.add(latency_stats, t_latency_stats)
            if t_max_latency > max_latency:
                max_latency = t_max_latency
            if t_min_latency < min_latency:
                min_latency = t_min_latency

        if is_copy:
            copyargs = query_args[-1]

            rowcount = await admin_conn.fetchval('''
                SELECT
                    count(*)
                FROM
                    "{tabname}"
            '''.format(tabname=copyargs['table']))

            print(rowcount, file=sys.stderr)

            if rowcount < len(query_args[0]) * queries:
                raise RuntimeError(
                    'COPY did not insert the expected number of rows')

        return {
            'queries': queries,
            'rows': rows,
            'duration': duration,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'latency_stats': latency_stats.tolist(),
        }

    finally:
        if teardown:
            await admin_conn.execute(teardown)


def die(msg):
    print('fatal: {}'.format(msg), file=sys.stderr)
    sys.exit(1)


async def save_summary_result(args):
    result_sum = defaultdict(dict)

    for query_file_path in (Path("./") / "queries").iterdir():
        data = query_file_path.read_text()
        query_data = json.loads(data)
        for driver in (AsyncPG, PsycoPG, AsyncPsycoPG):
            bench_results = await runner(
                args=args,
                driver=driver,
                query=query_data["query"],
                query_args=query_data.get("args", ()),
                setup=query_data.get("setup"),
                teardown=query_data.get("teardown"),
            )
            result_sum[query_file_path.name][driver.__name__] = bench_results

    with Path('./summary_bench_result.json').open("w") as f:
        json.dump(result_sum, f)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser(
        description='async pg driver benchmark [concurrent]')
    parser.add_argument(
        '-C', '--concurrency', type=int, default=10,
        help='number of concurrent connections')
    parser.add_argument(
        '-D', '--duration', type=int, default=30,
        help='duration of test in seconds')
    parser.add_argument(
        '--timeout', default=2, type=int,
        help='server timeout in seconds')
    parser.add_argument(
        '--warmup-time', type=int, default=5,
        help='duration of warmup period for each benchmark in seconds')

    coro = save_summary_result(parser.parse_args())
    loop.run_until_complete(coro)
