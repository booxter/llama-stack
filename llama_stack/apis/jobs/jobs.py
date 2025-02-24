# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.

from typing import List, Protocol, runtime_checkable

from pydantic import BaseModel

from llama_stack.schema_utils import json_schema_type, webmethod


# TODO: think through what should actually be exposed
@json_schema_type
class JobInfo(BaseModel):
    uuid: str
    type: str


class ListJobsResponse(BaseModel):
    data: List[JobInfo]


@runtime_checkable
class Jobs(Protocol):
    @webmethod(route="/jobs", method="GET")
    async def list_jobs(self) -> ListJobsResponse: ...

    @webmethod(route="/jobs/{job_id}", method="DELETE")
    async def delete_job(
        self,
        job_id: str,
    ) -> None: ...

    @webmethod(route="/jobs/{job_id}/cancel", method="POST")
    async def cancel_job(
        self,
        job_id: str,
    ) -> None: ...
