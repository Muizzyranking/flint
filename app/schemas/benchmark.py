from pydantic import BaseModel, Field, field_validator


class BenchmarkRequest(BaseModel):
    n: int = Field(
        default=10000,
        ge=100,
        le=1_000_000,
        description="Number of jobs to insert and pop during the benchmark.",
    )
    algorithm: str = Field(
        default="both",
        description="Which algorithm to benchmark: 'heap', 'timing_wheel', or 'both'.",
    )

    @field_validator("algorithm")
    @classmethod
    def validate_algorithm(cls, v: str) -> str:
        if v not in ("heap", "timing_wheel", "both"):
            return "both"
        return v


class AlgorithmResult(BaseModel):
    insert_time_ms: float
    pop_time_ms: float
    total_time_ms: float


class BenchmarkResult(BaseModel):
    n: int
    heap: AlgorithmResult | None = None
    timing_wheel: AlgorithmResult | None = None
    winner: str | None = None
    notes: str
