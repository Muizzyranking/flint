"""
Initial schema: jobs, job_dependencies, job_logs, settings

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-09 00:00:00.000000

"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------ #
    # jobs
    # ------------------------------------------------------------------ #
    op.create_table(
        "jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
            nullable=False,
        ),
        sa.Column("type", sa.String(100), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("priority", sa.SmallInteger(), nullable=False, server_default="2"),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column(
            "scheduled_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("interval_seconds", sa.BigInteger(), nullable=True),
        sa.Column("retry_count", sa.SmallInteger(), nullable=False, server_default="0"),
        sa.Column("max_retries", sa.SmallInteger(), nullable=False, server_default="3"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "effective_priority",
            sa.Float(),
            nullable=False,
            server_default="2.0",
        ),
        sa.Column(
            "cancellation_requested",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("worker_id", sa.String(100), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "is_dlq",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("priority IN (1, 2, 3)", name="ck_jobs_priority"),
        sa.CheckConstraint(
            "status IN ('pending','processing','completed','failed','cancelled')",
            name="ck_jobs_status",
        ),
    )

    # Indexes on jobs
    op.create_index(
        "idx_jobs_status",
        "jobs",
        ["status"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "idx_jobs_scheduled_at",
        "jobs",
        ["scheduled_at"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "idx_jobs_effective_priority",
        "jobs",
        ["effective_priority"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "idx_jobs_is_dlq",
        "jobs",
        ["is_dlq"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index("idx_jobs_deleted_at", "jobs", ["deleted_at"])
    op.create_index(
        "idx_jobs_type",
        "jobs",
        ["type"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "idx_jobs_worker_id",
        "jobs",
        ["worker_id"],
        postgresql_where=sa.text("worker_id IS NOT NULL"),
    )

    # ------------------------------------------------------------------ #
    # job_dependencies
    # ------------------------------------------------------------------ #
    op.create_table(
        "job_dependencies",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
            nullable=False,
        ),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "depends_on_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("job_id", "depends_on_id", name="uq_job_dependency"),
    )

    op.create_index("idx_job_deps_job_id", "job_dependencies", ["job_id"])
    op.create_index("idx_job_deps_depends_on_id", "job_dependencies", ["depends_on_id"])

    # ------------------------------------------------------------------ #
    # job_logs
    # ------------------------------------------------------------------ #
    op.create_table(
        "job_logs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
            nullable=False,
        ),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event", sa.String(50), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_index("idx_job_logs_job_id", "job_logs", ["job_id"])
    op.create_index("idx_job_logs_event", "job_logs", ["event"])
    op.create_index("idx_job_logs_created_at", "job_logs", ["created_at"])

    # ------------------------------------------------------------------ #
    # settings
    # ------------------------------------------------------------------ #
    op.create_table(
        "settings",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
            nullable=False,
        ),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("key", name="uq_settings_key"),
    )

    op.create_index("idx_settings_key", "settings", ["key"])

    # ------------------------------------------------------------------ #
    # Seed default settings
    # ------------------------------------------------------------------ #
    op.bulk_insert(
        sa.table(
            "settings",
            sa.column("id", postgresql.UUID(as_uuid=True)),
            sa.column("key", sa.String),
            sa.column("value", sa.Text),
            sa.column("description", sa.Text),
            sa.column("created_at", sa.String),
            sa.column("updated_at", sa.String),
        ),
        [
            {
                "id": uuid.uuid4(),
                "key": "dlq_threshold",
                "value": "5",
                "description": (
                    "Number of DLQ jobs that triggers an alert email. "
                    "Configurable at runtime via PATCH /api/v1/settings."
                ),
            },
            {
                "id": uuid.uuid4(),
                "key": "alert_emails",
                "value": "[]",
                "description": (
                    "JSON array of email addresses to receive DLQ threshold alerts. "
                    'Example: ["admin@example.com", "oncall@example.com"]'
                ),
            },
            {
                "id": uuid.uuid4(),
                "key": "scheduler_strategy",
                "value": "heap",
                "description": (
                    "Active scheduling algorithm used by workers and the scheduler. "
                    "Accepted values: 'heap' or 'timing_wheel'. "
                    "Switching takes effect on the next worker poll cycle."
                ),
            },
        ],
    )


def downgrade() -> None:
    op.drop_index("idx_job_logs_created_at", table_name="job_logs")
    op.drop_index("idx_job_logs_event", table_name="job_logs")
    op.drop_index("idx_job_logs_job_id", table_name="job_logs")
    op.drop_table("job_logs")

    op.drop_index("idx_job_deps_depends_on_id", table_name="job_dependencies")
    op.drop_index("idx_job_deps_job_id", table_name="job_dependencies")
    op.drop_table("job_dependencies")

    op.drop_index("idx_settings_key", table_name="settings")
    op.drop_table("settings")

    op.drop_index("idx_jobs_priority", table_name="jobs")
    op.drop_index("idx_jobs_worker_id", table_name="jobs")
    op.drop_index("idx_jobs_type", table_name="jobs")
    op.drop_index("idx_jobs_deleted_at", table_name="jobs")
    op.drop_index("idx_jobs_is_dlq", table_name="jobs")
    op.drop_index("idx_jobs_effective_priority", table_name="jobs")
    op.drop_index("idx_jobs_scheduled_at", table_name="jobs")
    op.drop_index("idx_jobs_status", table_name="jobs")
    op.drop_table("jobs")
