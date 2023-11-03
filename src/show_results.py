import json
import functools as ft
import typing as t

import numpy as np


class BenchResult(t.NamedTuple):
    asyncpg: "BenchResultStats"
    psycopg: "BenchResultStats"
    psycopg_async: "BenchResultStats"

    def make_table(self) -> str:
        return "\n".join([
            "| driver | queries | rows | duration | min_latency | max_latency | latency_median | latency_percentile_90 |",
            "|--|--|--|--|--|--|--|--|",
            *(
                (
                    f"| {name} | {driver.queries} | {driver.rows} | {driver.duration} | "
                    f"{driver.min_latency} | {driver.max_latency} | {driver.latency_median} | {driver.latency_percentile_90} |"
                )
                for name, driver in (
                    ("asyncpg", self.asyncpg),
                    ("psycopg", self.psycopg),
                    ("psycopg_async", self.psycopg_async),
                )
            ),
        ])


class BenchResultStats(t.NamedTuple):
    queries: int
    rows: int
    duration: float
    min_latency: float
    max_latency: float
    latency_percentile_90: float
    latency_median: float

    @classmethod
    def from_dict(cls, d):
        latency_stats = d["latency_stats"]
        latency_stats = ft.reduce(
            lambda a, b: (*a, *b),
            (
                [idx] * int(latency_stats[idx])
                for idx in range(len(latency_stats))
            ),
            (),
        )
        median, percentile_90 = np.percentile(latency_stats, (50, 90))
        return cls(
            queries=d["queries"],
            rows=d["rows"],
            duration=round(d["duration"], 2),
            min_latency=round(d["min_latency"], 2),
            max_latency=round(d["max_latency"], 2),
            latency_percentile_90=round(percentile_90, 2),
            latency_median=round(median, 2),
        )


with open("./summary_bench_result.json") as f:
    bench_results = json.load(f)

parsed_results = {}

for test_name in bench_results:
    parsed_results[test_name] = BenchResult(
        asyncpg=BenchResultStats.from_dict(bench_results[test_name]["AsyncPG"]),
        psycopg=BenchResultStats.from_dict(bench_results[test_name]["PsycoPG"]),
        psycopg_async=BenchResultStats.from_dict(bench_results[test_name]["AsyncPsycoPG"]),
    )

for test_name, bench_result in parsed_results.items():
    print(f"### {test_name}")
    print()
    print(bench_result.make_table())
    print()
