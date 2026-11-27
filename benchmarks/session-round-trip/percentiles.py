"""Rank every scheduled reply by its fixed deadline and actual completion."""

import csv
from decimal import Decimal
from statistics import median

PERCENTILES = [str(n) for n in range(1, 100)] + [
    "99.7",
    "99.9",
    "99.97",
    "99.99",
    "99.997",
    "99.999",
    "100",
]


def rank(samples):
    if not samples:
        raise ValueError("Empty sample population")
    ordered = sorted(samples)
    return {
        p: ordered[(int(Decimal(p) * 1000) * len(ordered) + 99999) // 100000 - 1] for p in PERCENTILES
    }


def summarize(rows, output, warmup, rate, count, runs):
    groups = [[[], []] for _ in range(runs)]
    in_window = [0] * runs
    seen = 0
    for expected, (index, lane, due, ack) in enumerate(rows):
        if index != expected or lane != index % 2 or due <= 0 or ack < due:
            raise ValueError(f"Invalid identity or latency at row {expected}")
        seen += 1
        if index < warmup:
            window, slot = -1, index
        else:
            window, slot = divmod(index - warmup, count)
        if slot == 0:
            origin = due
        if due != origin + slot * 1_000_000_000 // rate or window >= runs:
            raise ValueError(f"Invalid schedule at row {index}")
        if window >= 0:
            groups[window][lane].append(ack - due)
            in_window[window] += ack <= origin + count * 1_000_000_000 // rate
    if seen != warmup + runs * count:
        raise ValueError(f"Expected {warmup + runs * count} replies, saw {seen}")
    with (output / "percentiles.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "sessions",
                "percentile",
                *[f"window_{n + 1}_us" for n in range(runs)],
                "median_us",
                "pooled_us",
            ]
        )
        for name, lane in [("both", None), ("lane0", 0), ("lane1", 1)]:
            windows = [left + right if lane is None else (left, right)[lane] for left, right in groups]
            stats = [rank(window) for window in windows]
            pooled = rank([v for window in windows for v in window])
            for p in PERCENTILES:
                values = [s[p] for s in stats]
                writer.writerow(
                    [name, p, *[v / 1000 for v in values], median(values) / 1000, pooled[p] / 1000]
                )
            if name == "both":
                print(
                    "Median of window p99 / p99.9 (ms)",
                    *(median(s[p] for s in stats) / 1e6 for p in ("99", "99.9")),
                )
    print("Completed within each window", in_window)
