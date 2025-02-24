# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.

import itertools

from pydantic import BaseModel

from llama_stack.apis.jobs import (
    JobInfo,
    JobArtifact,
    Jobs,
    ListJobsResponse,
)
from llama_stack.distribution.datatypes import StackRunConfig


_JOB_SCHEDULERS = []


# TODO: is there a better way to dynamically iterate over providers to extract
# their schedulers?..
def register_job_scheduler(scheduler):
    _JOB_SCHEDULERS.append(scheduler)


class DistributionJobsConfig(BaseModel):
    run_config: StackRunConfig


async def get_provider_impl(config, deps):
    impl = DistributionJobsImpl(config, deps)
    await impl.initialize()
    return impl


class DistributionJobsImpl(Jobs):
    def __init__(self, config, deps):
        self.config = config
        self.deps = deps

    async def initialize(self) -> None:
        pass

    def _job_to_job_info(self, job):
        return JobInfo(
            uuid=job.id,
            type=job.type,
            status=job.status,

            scheduled_at=job.scheduled_at,
            started_at=job.started_at,
            completed_at=job.completed_at,

            artifacts=[JobArtifact(
                name=artifact['name'],
                type=artifact['type'],
                uri=artifact['uri'],
                metadata=dict(artifact['metadata']),
            ) for artifact in job.artifacts],
        )

    async def list_jobs(self) -> ListJobsResponse:
        jobs = list(itertools.chain(*[
            scheduler.get_jobs()
            for scheduler in _JOB_SCHEDULERS
        ]))
        return ListJobsResponse(data=[
            self._job_to_job_info(job)
            for job in jobs
        ])

    # TODO: this should be improved
    async def delete_job(self, job_id: str) -> None:
        for scheduler in _JOB_SCHEDULERS:
            try:
                job = scheduler.get_job(job_id)
            except ValueError:
                continue
            if job is not None:
                scheduler.delete(job_id)
                break
        # TODO: raise error if job not found

    async def cancel_job(self, job_id: str) -> None:
        for scheduler in _JOB_SCHEDULERS:
            try:
                job = scheduler.get_job(job_id)
            except ValueError:
                continue
            if job is not None:
                scheduler.cancel(job_id)
                break
        # TODO: raise error if job not found

    async def get_job(self, job_id: str) -> JobInfo:
        for scheduler in _JOB_SCHEDULERS:
            try:
                job = scheduler.get_job(job_id)
            except ValueError:
                continue
            if job is not None:
                return self._job_to_job_info(job)
        # TODO: raise error if job not found

    async def shutdown(self) -> None:
        pass
