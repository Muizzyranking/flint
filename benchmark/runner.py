import argparse
import asyncio
import random
import time

from app.queues.heapq import HeapQueue
from app.queues.timing_wheel import TimingWheel


async def run_benchmark(
    n: int = 10000,
    algorithm: str = "both",
) -> dict:
    """
    Benchmark HeapQueue and/or TimingWheel.

    Args:
        n:         Number of jobs to insert and pop.
        algorithm: 'heap', 'timing_wheel', or 'both'.

    Returns:
        Dict with timing results and winner.
    """
    now = time.time()
    jobs = [
        (
            str(i),
            float(random.randint(1, 3)),
            now + random.uniform(0, 3600),
            now - random.uniform(0, 600),
        )
        for i in range(n)
    ]

    results: dict = {"n": n}

    # =========================
    # Heap benchmark
    # =========================
    if algorithm in ("heap", "both"):
        heap = HeapQueue()

        t0 = time.perf_counter()
        for job_id, ep, sa, ca in jobs:
            await heap.push(job_id, ep, sa, ca)
        insert_time = time.perf_counter() - t0

        t0 = time.perf_counter()
        while await heap.size() > 0:
            await heap.pop()
        pop_time = time.perf_counter() - t0

        results["heap"] = {
            "insert_time_ms": round(insert_time * 1000, 2),
            "pop_time_ms": round(pop_time * 1000, 2),
            "total_time_ms": round((insert_time + pop_time) * 1000, 2),
        }

    # =========================
    # Timing wheel benchmark
    # =========================
    if algorithm in ("timing_wheel", "both"):
        wheel = TimingWheel()

        # Insert
        t0 = time.perf_counter()
        for job_id, ep, sa, ca in jobs:
            await wheel.push(job_id, ep, sa, ca)
        insert_time = time.perf_counter() - t0

        # Drain — tick until empty
        t0 = time.perf_counter()
        while await wheel.size() > 0:
            await wheel.tick()
        pop_time = time.perf_counter() - t0

        results["timing_wheel"] = {
            "insert_time_ms": round(insert_time * 1000, 2),
            "pop_time_ms": round(pop_time * 1000, 2),
            "total_time_ms": round((insert_time + pop_time) * 1000, 2),
        }

    # Winner
    if algorithm == "both" and "heap" in results and "timing_wheel" in results:
        heap_total = results["heap"]["total_time_ms"]
        tw_total = results["timing_wheel"]["total_time_ms"]
        results["winner"] = "timing_wheel" if tw_total < heap_total else "heap"
        results["notes"] = (
            f"At n={n}: timing_wheel total={tw_total}ms, heap total={heap_total}ms. "
            "Timing wheel wins on raw insert/pop throughput (O(1) vs O(log n)). "
            "Heap wins on priority ordering correctness and re-scoring efficiency "
            "(aging process). For Flint's workload — priority + starvation prevention "
            "— heap is the correct primary algorithm."
        )
    elif algorithm == "heap":
        results["winner"] = "heap"
        results["notes"] = "Only heap was benchmarked."
    else:
        results["winner"] = "timing_wheel"
        results["notes"] = "Only timing wheel was benchmarked."

    return results


def _print_results(results: dict) -> None:
    n = results["n"]
    print(f"\n{'=' * 55}")
    print(f"  FLINT BENCHMARK RESULTS  (n={n:,})")
    print(f"{'=' * 55}")

    for algo, label in [("heap", "HEAP"), ("timing_wheel", "TIMING WHEEL")]:
        if algo in results:
            r = results[algo]
            print(f"\n  {label}")
            print(f"    Insert : {r['insert_time_ms']:>10.2f} ms")
            print(f"    Pop    : {r['pop_time_ms']:>10.2f} ms")
            print(f"    Total  : {r['total_time_ms']:>10.2f} ms")

    if "winner" in results:
        print(f"\n  Winner (total time): {results['winner'].upper().replace('_', ' ')}")
        print("\n  Analysis:")
        words = results["notes"].split()
        line = "    "
        for word in words:
            if len(line) + len(word) > 72:
                print(line)
                line = "    " + word + " "
            else:
                line += word + " "
        if line.strip():
            print(line)

    print(f"\n{'=' * 55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flint — Queue Algorithm Benchmark")
    parser.add_argument(
        "--n",
        type=int,
        default=10000,
        help="Number of jobs to benchmark (default: 10000)",
    )
    parser.add_argument(
        "--algorithm",
        choices=["heap", "timing_wheel", "both"],
        default="both",
        help="Algorithm to benchmark (default: both)",
    )
    args = parser.parse_args()

    results = asyncio.run(run_benchmark(n=args.n, algorithm=args.algorithm))
    _print_results(results)
