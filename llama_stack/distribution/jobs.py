# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.

import itertools

from pydantic import BaseModel

from llama_stack.apis.jobs import (
    JobInfo,
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

    async def list_jobs(self) -> ListJobsResponse:
        job_uuids = list(itertools.chain(*[
            scheduler.get_jobs()
            for scheduler in _JOB_SCHEDULERS
        ]))
        return ListJobsResponse(data=[JobInfo(uuid=job) for job in job_uuids])

    async def shutdown(self) -> None:
        pass
