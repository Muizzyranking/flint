import uuid
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import DependencyCycleException, DependencyNotFoundException
from app.core.logger import get_logger
from app.models.job import Job, JobStatus
from app.models.job_depedencies import JobDependency
from app.models.job_log import JobLog, LogEvent

if TYPE_CHECKING:
    from app.queues.base import BaseQueue

logger = get_logger(__name__)


async def check_cycle(
    new_job_id: uuid.UUID,
    dependency_ids: list[uuid.UUID],
    db: AsyncSession,
) -> None:
    """
    Verify that adding dependency_ids to new_job_id does not create a cycle.

    Performs a depth-first search starting from each dependency_id,
    traversing its own dependencies recursively. If new_job_id is
    encountered during the traversal, a cycle would be created.

    Raises:
        DependencyNotFoundException: If any dependency_id does not exist.
        DependencyCycleException:    If a cycle would be introduced.
    """
    for dep_id in dependency_ids:
        result = await db.execute(
            select(Job.id).where(
                Job.id == dep_id,
                Job.deleted_at.is_(None),
            )
        )
        if result.scalar_one_or_none() is None:
            raise DependencyNotFoundException(str(dep_id))

    for dep_id in dependency_ids:
        visited: set[uuid.UUID] = set()
        await _dfs_cycle_check(
            start=dep_id,
            target=new_job_id,
            visited=visited,
            db=db,
        )


async def _dfs_cycle_check(
    start: uuid.UUID,
    target: uuid.UUID,
    visited: set[uuid.UUID],
    db: AsyncSession,
) -> None:
    """
    Recursive DFS. Raises DependencyCycleException if target is found.
    """
    if start == target:
        raise DependencyCycleException(str(target), str(start))

    if start in visited:
        return
    visited.add(start)

    result = await db.execute(
        select(JobDependency.depends_on_id).where(JobDependency.job_id == start)
    )
    upstream_ids = [row[0] for row in result.fetchall()]

    for upstream_id in upstream_ids:
        await _dfs_cycle_check(
            start=upstream_id,
            target=target,
            visited=visited,
            db=db,
        )


async def create_dependencies(
    job_id: uuid.UUID,
    dependency_ids: list[uuid.UUID],
    db: AsyncSession,
) -> None:
    """
    Insert rows into job_dependencies for all dependency_ids.
    Assumes cycle check has already been performed.
    """
    for dep_id in dependency_ids:
        dependency = JobDependency(
            job_id=job_id,
            depends_on_id=dep_id,
        )
        db.add(dependency)

    await db.flush()
    logger.info(
        "job_dependencies_created",
        job_id=str(job_id),
        dependency_count=len(dependency_ids),
        dependency_ids=[str(d) for d in dependency_ids],
    )


async def on_job_completed(
    job_id: uuid.UUID,
    db: AsyncSession,
    queue: "BaseQueue",
) -> None:
    """
    Called after every successful job completion.
    """
    result = await db.execute(
        select(JobDependency.job_id).where(JobDependency.depends_on_id == job_id)
    )
    dependent_job_ids = [row[0] for row in result.fetchall()]

    for dependent_id in dependent_job_ids:
        await _maybe_unblock_job(dependent_id, db, queue)


async def _maybe_unblock_job(
    job_id: uuid.UUID,
    db: AsyncSession,
    queue: "BaseQueue",
) -> None:
    """
    Check if all dependencies of job_id are completed.
    If yes, push the job onto the queue.
    """
    result = await db.execute(
        select(Job).where(
            Job.id == job_id,
            Job.deleted_at.is_(None),
        )
    )
    job = result.scalar_one_or_none()
    if not job or job.status != JobStatus.PENDING:
        return

    total_result = await db.execute(
        select(func.count(JobDependency.id)).where(JobDependency.job_id == job_id)
    )
    total_deps = total_result.scalar() or 0

    if total_deps == 0:
        return

    # Count completed dependencies
    completed_result = await db.execute(
        select(func.count(Job.id))
        .join(JobDependency, Job.id == JobDependency.depends_on_id)
        .where(
            JobDependency.job_id == job_id,
            Job.status == JobStatus.COMPLETED,
        )
    )
    completed_count = completed_result.scalar() or 0

    if completed_count == total_deps:
        logger.info(
            "job_unblocked",
            job_id=str(job_id),
            completed_deps=completed_count,
        )
        await queue.push(
            job_id=str(job_id),
            effective_priority=job.effective_priority,
            scheduled_at=job.scheduled_at.timestamp(),
            created_at=job.created_at.timestamp(),
        )


async def on_job_failed(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """
    Called when a job exhausts its retries and moves to the DLQ.
    """
    await _cascade_cancel(job_id, db)


async def _cascade_cancel(
    failed_job_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """
    BFS through the dependency graph cancelling all downstream jobs.
    Only cancels jobs that are still 'pending'.
    """
    queue: list[uuid.UUID] = [failed_job_id]
    visited: set[uuid.UUID] = {failed_job_id}

    while queue:
        current_id = queue.pop(0)

        # Find all jobs that directly depend on current_id
        result = await db.execute(
            select(JobDependency.job_id).where(
                JobDependency.depends_on_id == current_id
            )
        )
        dependent_ids = [row[0] for row in result.fetchall()]

        for dep_id in dependent_ids:
            if dep_id in visited:
                continue
            visited.add(dep_id)

            update_result = await db.execute(
                update(Job)
                .where(
                    Job.id == dep_id,
                    Job.status == JobStatus.PENDING,
                    Job.deleted_at.is_(None),
                )
                .values(
                    status=JobStatus.CANCELLED,
                    last_error=(
                        f"Cancelled: dependency job {failed_job_id} "
                        f"failed permanently and was moved to DLQ."
                    ),
                    updated_at=func.now(),
                )
                .returning(Job.id)
            )
            cancelled_id = update_result.scalar_one_or_none()

            if cancelled_id:
                log_entry = JobLog(
                    job_id=dep_id,
                    event=LogEvent.JOB_CANCELLED,
                    message=(
                        f"Job automatically cancelled because upstream "
                        f"dependency {failed_job_id} failed permanently."
                    ),
                    metadata_={
                        "failed_dependency_id": str(failed_job_id),
                        "reason": "upstream_dependency_failed",
                    },
                )
                db.add(log_entry)

                logger.info(
                    "job_cascade_cancelled",
                    job_id=str(dep_id),
                    failed_dependency=str(failed_job_id),
                )

            queue.append(dep_id)

    await db.commit()


async def on_dag_root_retried(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """
    Called when manually retries a DLQ job that has downstream dependents.
    """
    await _cascade_reset(job_id, db)


async def _cascade_reset(
    retried_job_id: uuid.UUID,
    session: AsyncSession,
) -> None:
    """
    BFS through the dependency graph resetting auto-cancelled downstream jobs.
    """
    queue: list[uuid.UUID] = [retried_job_id]
    visited: set[uuid.UUID] = {retried_job_id}

    while queue:
        current_id = queue.pop(0)

        result = await session.execute(
            select(JobDependency.job_id).where(
                JobDependency.depends_on_id == current_id
            )
        )
        dependent_ids = [row[0] for row in result.fetchall()]

        for dep_id in dependent_ids:
            if dep_id in visited:
                continue
            visited.add(dep_id)

            # Only reset jobs that were auto-cancelled due to dependency failure
            dep_result = await session.execute(
                select(Job).where(
                    Job.id == dep_id,
                    Job.deleted_at.is_(None),
                )
            )
            dep_job = dep_result.scalar_one_or_none()

            if not dep_job:
                continue

            is_auto_cancelled = (
                dep_job.status == JobStatus.CANCELLED
                and dep_job.last_error is not None
                and "dependency" in dep_job.last_error.lower()
            )

            if is_auto_cancelled:
                await session.execute(
                    update(Job)
                    .where(Job.id == dep_id)
                    .values(
                        status=JobStatus.PENDING,
                        retry_count=0,
                        last_error=None,
                        worker_id=None,
                        updated_at=func.now(),
                    )
                )

                log_entry = JobLog(
                    job_id=dep_id,
                    event=LogEvent.JOB_CREATED,
                    message=(
                        f"Job reset to pending: upstream dependency "
                        f"{retried_job_id} was manually retried."
                    ),
                    metadata_={
                        "retried_dependency_id": str(retried_job_id),
                        "reason": "upstream_dependency_retried",
                    },
                )
                session.add(log_entry)

                logger.info(
                    "job_cascade_reset",
                    job_id=str(dep_id),
                    retried_dependency=str(retried_job_id),
                )

            queue.append(dep_id)

    await session.commit()


async def get_dependency_ids(
    job_id: uuid.UUID,
    db: AsyncSession,
) -> list[uuid.UUID]:
    """Return the list of job IDs that job_id depends on."""
    result = await db.execute(
        select(JobDependency.depends_on_id).where(JobDependency.job_id == job_id)
    )
    return [row[0] for row in result.fetchall()]


async def has_unmet_dependencies(
    job_id: uuid.UUID,
    session: AsyncSession,
) -> bool:
    """
    Return True if job_id has at least one dependency that is not yet completed.
    """
    result = await session.execute(
        select(func.count(JobDependency.id))
        .join(Job, Job.id == JobDependency.depends_on_id)
        .where(
            JobDependency.job_id == job_id,
            Job.status != JobStatus.COMPLETED,
        )
    )
    unmet_count = result.scalar() or 0
    return unmet_count > 0
