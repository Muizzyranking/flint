"""Distributed dispatch: availability, leases and recurring idempotency.

Revision ID: 0002_distributed_dispatch
Revises: 0001_initial
Create Date: 2026-10-24 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0002_distributed_dispatch"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.execute("UPDATE jobs SET available_at = COALESCE(next_retry_at, scheduled_at)")
    op.alter_column(
        "jobs",
        "available_at",
        nullable=False,
        server_default=sa.func.now(),
    )

    op.add_column(
        "jobs",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "recurrence_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "recurrence_run",
            sa.SmallInteger(),
            nullable=False,
            server_default="0",
        ),
    )

    op.create_index("idx_jobs_priority", "jobs", ["priority"])
    op.create_index("idx_jobs_lease_expires_at", "jobs", ["lease_expires_at"])
    op.create_index("idx_jobs_recurrence_id", "jobs", ["recurrence_id"])
    op.create_index(
        "idx_jobs_dispatch",
        "jobs",
        ["status", "available_at", "effective_priority", "created_at"],
        postgresql_where=sa.text("deleted_at IS NULL AND is_dlq = false"),
    )
    op.create_unique_constraint(
        "uq_jobs_recurrence_run",
        "jobs",
        ["recurrence_id", "recurrence_run"],
    )

    # The scheduler strategy setting no longer exists: there is one durable
    # dispatch path and one local ordering algorithm.
    op.execute("DELETE FROM settings WHERE key = 'scheduler_strategy'")


def downgrade() -> None:
    op.execute(
        "INSERT INTO settings (id, key, value, description, created_at, updated_at) "
        "VALUES (gen_random_uuid(), 'scheduler_strategy', 'heap', "
        "'Active scheduling algorithm.', now(), now()) "
        "ON CONFLICT (key) DO NOTHING"
    )

    op.drop_constraint("uq_jobs_recurrence_run", "jobs", type_="unique")
    op.drop_index("idx_jobs_dispatch", table_name="jobs")
    op.drop_index("idx_jobs_recurrence_id", table_name="jobs")
    op.drop_index("idx_jobs_lease_expires_at", table_name="jobs")
    op.drop_index("idx_jobs_priority", table_name="jobs")

    op.drop_column("jobs", "recurrence_run")
    op.drop_column("jobs", "recurrence_id")
    op.drop_column("jobs", "lease_expires_at")
    op.drop_column("jobs", "available_at")
