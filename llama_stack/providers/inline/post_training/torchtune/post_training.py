# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.
# from datetime import datetime
import os
from typing import Any, Dict, Optional

from llama_stack.apis.datasetio import DatasetIO
from llama_stack.apis.datasets import Datasets
from llama_stack.apis.post_training import (
    AlgorithmConfig,
    DPOAlignmentConfig,
    JobStatus,
    ListPostTrainingJobsResponse,
    LoraFinetuningConfig,
    PostTrainingJob,
    PostTrainingJobArtifactsResponse,
    PostTrainingJobStatusResponse,
    TrainingConfig,
)
from llama_stack.distribution.jobs import register_job_scheduler
from llama_stack.providers.inline.post_training.torchtune.config import (
    TorchtunePostTrainingConfig,
)
from llama_stack.providers.inline.post_training.torchtune.recipes.lora_finetuning_single_device import (
    LoraFinetuningSingleDevice,
)
from llama_stack.schema_utils import webmethod

from .scheduler import Job, Scheduler
from .scheduler import JobStatus as SchedulerJobStatus

os.environ["OMP_NUM_THREADS"] = "1"  # Or set to a lower number


class TorchtunePostTrainingImpl:
    def __init__(
        self,
        config: TorchtunePostTrainingConfig,
        datasetio_api: DatasetIO,
        datasets: Datasets,
    ) -> None:
        self.config = config
        self.datasetio_api = datasetio_api
        self.datasets_api = datasets

        self._scheduler = Scheduler()
        register_job_scheduler(self._scheduler)

    async def supervised_fine_tune(
        self,
        job_uuid: str,  # TODO: remove job_uuid from API (at least don't require it)
        training_config: TrainingConfig,
        hyperparam_search_config: Dict[str, Any],
        logger_config: Dict[str, Any],
        model: str,
        checkpoint_dir: Optional[str],
        algorithm_config: Optional[AlgorithmConfig],
    ) -> PostTrainingJob:
        if any(job.id == job_uuid for job in self._scheduler.get_jobs()):
            raise ValueError(f"Job {job_uuid} already exists")

        if isinstance(algorithm_config, LoraFinetuningConfig):

            async def handler(on_log_message_cb, on_status_change_cb, on_artifact_collected_cb):
                # TODO: try on_log_message_cb here to confirm it works
                on_log_message_cb("Starting job")

                on_log_message_cb("Setting up recipe...")
                recipe = LoraFinetuningSingleDevice(
                    self.config,
                    job_uuid,
                    training_config,
                    hyperparam_search_config,
                    logger_config,
                    model,
                    checkpoint_dir,
                    algorithm_config,
                    self.datasetio_api,
                    self.datasets_api,
                )
                await recipe.setup()
                on_log_message_cb("Recipe setup complete")

                on_log_message_cb("Training model...")
                # TODO: what to do with resources_allocated?
                resources_allocated, checkpoints = await recipe.train()
                on_log_message_cb("Training complete")

                on_log_message_cb("Collecting artifacts...")
                for checkpoint in checkpoints:
                    on_artifact_collected_cb(checkpoint.identifier, "checkpoint", checkpoint.path, checkpoint)
                on_log_message_cb("Artifacts collected")

                # TODO: scheduler should probably control the completion status instead
                on_status_change_cb(SchedulerJobStatus.completed)
        else:
            raise NotImplementedError()

        print("Scheduling job with uuid", job_uuid)
        job_uuid = self._scheduler.schedule(Job("supervised-fine-tune", handler), job_uuid=job_uuid)
        print("Scheduled job with uuid", job_uuid)
        return PostTrainingJob(job_uuid=job_uuid)

    async def preference_optimize(
        self,
        job_uuid: str,
        finetuned_model: str,
        algorithm_config: DPOAlignmentConfig,
        training_config: TrainingConfig,
        hyperparam_search_config: Dict[str, Any],
        logger_config: Dict[str, Any],
    ) -> PostTrainingJob: ...

    async def get_training_jobs(self) -> ListPostTrainingJobsResponse:
        return ListPostTrainingJobsResponse(
            data=[PostTrainingJob(job_uuid=job.id) for job in self._scheduler.get_jobs()]
        )

    # TODO: fix handling of artifacts (e.g. they may not be just checkpoints)
    @staticmethod
    def _get_checkpoints(job):
        return [artifact['metadata'] for artifact in job.artifacts]

    @webmethod(route="/post-training/job/status")
    async def get_training_job_status(self, job_uuid: str) -> Optional[PostTrainingJobStatusResponse]:
        job = self._scheduler.get_job(job_uuid)

        # TODO: cover all options
        match job.status:
            case SchedulerJobStatus.new:
                status = JobStatus.scheduled
            case SchedulerJobStatus.scheduled:
                status = JobStatus.scheduled
            case SchedulerJobStatus.running:
                status = JobStatus.in_progress
            case SchedulerJobStatus.completed:
                status = JobStatus.completed
            case SchedulerJobStatus.failed:
                status = JobStatus.failed
            case _:
                raise NotImplementedError()

        return PostTrainingJobStatusResponse(
            job_uuid=job_uuid,
            status=status,
            scheduled_at=job.scheduled_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            checkpoints=self._get_checkpoints(job),
        )

    @webmethod(route="/post-training/job/cancel")
    async def cancel_training_job(self, job_uuid: str) -> None:
        self._scheduler.cancel(job_uuid)

    @webmethod(route="/post-training/job/artifacts")
    async def get_training_job_artifacts(self, job_uuid: str) -> Optional[PostTrainingJobArtifactsResponse]:
        job = self._scheduler.get_job(job_uuid)
        return PostTrainingJobArtifactsResponse(job_uuid=job_uuid, checkpoints=self._get_checkpoints(job))
