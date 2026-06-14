# Flint — Algorithm Benchmark
> Heap-based priority queue vs Timing Wheel scheduling algorithm.

---

## Overview

Flint implements two scheduling algorithms:

- **HeapQueue** — the primary dispatcher used in production job dispatch
- **TimingWheel** — an alternative algorithm included for benchmarking and comparison

The benchmark measures insert time, pop/drain time, and total time for each algorithm at three scales: 1,000 / 10,000 / 100,000 jobs. All jobs are given random priorities (1–3) and random scheduled times within a 1-hour window.

---

## How to Run

```bash
# From the project root
python -m benchmark.runner --n 10000
python -m benchmark.runner --n 100000 --algorithm heap
python -m benchmark.runner --n 50000 --algorithm timing_wheel

# Or via the API (no restart needed)
POST /api/v1/benchmark/run
{ "n": 10000, "algorithm": "both" }
```

---

## Results

All measurements taken on a single machine. Times in milliseconds.

### n = 1,000 jobs

| Metric      | Heap     | Timing Wheel |
|-------------|----------|--------------|
| Insert      | 2.44 ms  | 2.26 ms      |
| Pop / Drain | 3.34 ms  | 409.84 ms    |
| **Total**   | **5.78 ms** | **412.10 ms** |
| Winner      | ✓ Heap   |              |

### n = 10,000 jobs

| Metric      | Heap     | Timing Wheel |
|-------------|----------|--------------|
| Insert      | 26.58 ms | 15.56 ms     |
| Pop / Drain | 42.44 ms | 413.60 ms    |
| **Total**   | **69.02 ms** | **429.15 ms** |
| Winner      | ✓ Heap   |              |

### n = 100,000 jobs

| Metric      | Heap      | Timing Wheel |
|-------------|-----------|--------------|
| Insert      | 285.02 ms | 294.74 ms    |
| Pop / Drain | 610.75 ms | 457.30 ms    |
| **Total**   | **895.77 ms** | **752.04 ms** |
| Winner      |           | ✓ Timing Wheel |

---

## Analysis

### Insert Performance

At small and medium scale (n ≤ 10,000), heap insert is slightly slower than the timing wheel because `heappush` is O(log n) while the timing wheel's slot placement is O(1) amortised. At n = 100,000 the two are essentially equal in insert time (~285ms vs ~295ms) — the difference is within noise.

### Pop / Drain Performance

This is where the algorithms diverge significantly. At small and medium scale, the heap's `heappop` (O(log n)) is dramatically faster than the timing wheel's drain loop. The timing wheel's `tick()` must advance through all 3,600 slots regardless of how many are occupied, making drain O(W) where W = wheel size (3,600). At n = 1,000 this produces a 122× slowdown vs the heap.

At n = 100,000 the timing wheel drain wins because the 3,600-slot scan cost becomes relatively smaller compared to the volume of jobs drained per tick.

### Why Heap Wins for Flint's Workload

The timing wheel's O(W) drain cost is a fixed overhead proportional to the wheel size, not the number of jobs. In Flint's typical workload — tens to hundreds of pending jobs at any given time — this fixed overhead makes the timing wheel 50–100× slower than the heap.

Beyond raw throughput, the heap provides two properties the timing wheel cannot:

**1. Exact global priority ordering**
The heap sorts by `(effective_priority, scheduled_at, created_at, job_id)` across the entire queue. The timing wheel only sorts within a single 1-second slot — jobs in different slots have no cross-slot priority comparison.

**2. Efficient re-scoring (starvation prevention)**
Flint's aging process decrements `effective_priority` on waiting jobs every 30 seconds. The heap handles this via lazy deletion in O(log n). The timing wheel would require scanning all slots to find and move a job — O(W) per re-scored job.

These two properties are first-class requirements for Flint. The heap is the correct primary algorithm.

### When the Timing Wheel Would Win

The timing wheel would be the better choice for a system where:

- Job volumes consistently exceed 100,000 items in the active queue
- All jobs have uniform or near-uniform priorities (no aging needed)
- Scheduling precision is the dominant concern over priority ordering
- The wheel size matches the scheduling horizon (e.g. 3,600 slots for 1-hour windows)

In such a system, the timing wheel's O(1) insert and amortised O(1) pop would outperform the heap's O(log n) at scale.

---

## Complexity Summary

| Operation | Heap | Timing Wheel |
|-----------|------|--------------|
| Insert | O(log n) | O(1) amortised |
| Pop next | O(log n) | O(W/n) amortised |
| Re-score (aging) | O(log n) lazy deletion | O(W) slot scan |
| Remove arbitrary | O(1) mark + O(log n) next pop | O(W) |
| Priority ordering | Exact global | Per-slot only |
| Memory | O(n) | O(W + n) |

Where n = number of jobs in queue, W = wheel size (3,600 for Flint's implementation).

---

## Implementation Notes

### HeapQueue (`app/queues/heap_queue.py`)

- Python `heapq` module with a `(effective_priority, scheduled_at, created_at, job_id)` tuple sort key
- `asyncio.Lock` protects all operations for concurrent coroutine safety
- Lazy deletion via `_entry_finder` dict — O(1) mark, stale entries discarded on pop
- `update_priority()` marks old entry removed and pushes new one — full re-score in O(log n)
- `contains()` — O(1) lookup via `_entry_finder`

### TimingWheel (`app/queues/timing_wheel.py`)

- 3,600 slots, 1-second resolution, covers 1 hour
- Each slot is a list of `(effective_priority, job_id)` tuples sorted ascending by priority
- Jobs beyond 3,600 seconds go into an overflow dict keyed by scheduled timestamp
- `tick()` advances the pointer, drains the current slot, pulls in overflow jobs within range
- `remove()` is O(W) — scans all slots (acceptable given the benchmark use case)
