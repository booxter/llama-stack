# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.
from enum import Enum
from typing import Any, Dict, Optional

from llama_stack.apis.datasetio import DatasetIO
from llama_stack.apis.datasets import Datasets
from llama_stack.apis.post_training import (
    AlgorithmConfig,
    Checkpoint,
    DPOAlignmentConfig,
    JobStatus,
    ListPostTrainingJobsResponse,
    LoraFinetuningConfig,
    PostTrainingJob,
    PostTrainingJobArtifactsResponse,
    PostTrainingJobStatusResponse,
    TrainingConfig,
)
from llama_stack.providers.inline.post_training.torchtune.config import (
    TorchtunePostTrainingConfig,
)
from llama_stack.providers.utils.scheduler import JobArtifact, Scheduler
from llama_stack.providers.utils.scheduler import JobStatus as SchedulerJobStatus
from llama_stack.schema_utils import webmethod

from .pipeline import pipeline


class TrainingArtifactType(Enum):
    CHECKPOINT = "checkpoint"
    RESOURCES_STATS = "resources_stats"


_JOB_TYPE_SUPERVISED_FINE_TUNE = "supervised-fine-tune"


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
        self._scheduler = Scheduler(backend="kubeflow")

    async def shutdown(self) -> None:
        await self._scheduler.shutdown()

    @staticmethod
    def _checkpoint_to_artifact(checkpoint: Checkpoint) -> JobArtifact:
        return JobArtifact(
            type=TrainingArtifactType.CHECKPOINT.value,
            name=checkpoint.identifier,
            uri=checkpoint.path,
            metadata=dict(checkpoint),
        )

    @staticmethod
    def _resources_stats_to_artifact(resources_stats: Dict[str, Any]) -> JobArtifact:
        return JobArtifact(
            type=TrainingArtifactType.RESOURCES_STATS.value,
            name=TrainingArtifactType.RESOURCES_STATS.value,
            metadata=resources_stats,
        )

    async def supervised_fine_tune(
        self,
        job_uuid: str,
        training_config: TrainingConfig,
        hyperparam_search_config: Dict[str, Any],
        logger_config: Dict[str, Any],
        model: str,
        checkpoint_dir: Optional[str],
        algorithm_config: Optional[AlgorithmConfig],
    ) -> PostTrainingJob:
        if not isinstance(algorithm_config, LoraFinetuningConfig):
            raise NotImplementedError()

        async def fetch_rows(dataset_id: str):
            return await self.datasetio_api.iterrows(
                dataset_id=dataset_id,
                limit=-1,
            )

        dataset_id = training_config.data_config.dataset_id
        all_rows = await fetch_rows(dataset_id)
        data = all_rows.data

        config = self.config.model_dump(exclude_none=True, mode="json")
        training_config_ = training_config.model_dump(exclude_none=True, mode="json")
        hyperparam_search_config = hyperparam_search_config
        logger_config = logger_config
        model = model
        checkpoint_dir = checkpoint_dir
        algorithm_config_ = algorithm_config.model_dump(exclude_none=True, mode="json")

        p = pipeline(
            config,
            data,
            job_uuid,
            training_config_,
            hyperparam_search_config,
            logger_config,
            model,
            checkpoint_dir or "null",
            algorithm_config_,
        )

        job_uuid = self._scheduler.schedule(_JOB_TYPE_SUPERVISED_FINE_TUNE, job_uuid, p)
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

    @staticmethod
    def _get_artifacts_metadata_by_type(job, artifact_type):
        return [artifact.metadata for artifact in job.artifacts if artifact.type == artifact_type]

    @classmethod
    def _get_checkpoints(cls, job):
        return cls._get_artifacts_metadata_by_type(job, TrainingArtifactType.CHECKPOINT.value)

    @classmethod
    def _get_resources_allocated(cls, job):
        data = cls._get_artifacts_metadata_by_type(job, TrainingArtifactType.RESOURCES_STATS.value)
        return data[0] if data else None

    @webmethod(route="/post-training/job/status")
    async def get_training_job_status(self, job_uuid: str) -> Optional[PostTrainingJobStatusResponse]:
        job = self._scheduler.get_job(job_uuid)

        match job.status:
            # TODO: Add support for other statuses to API
            case SchedulerJobStatus.new | SchedulerJobStatus.scheduled:
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
            resources_allocated=self._get_resources_allocated(job),
        )

    @webmethod(route="/post-training/job/cancel")
    async def cancel_training_job(self, job_uuid: str) -> None:
        self._scheduler.cancel(job_uuid)

    @webmethod(route="/post-training/job/artifacts")
    async def get_training_job_artifacts(self, job_uuid: str) -> Optional[PostTrainingJobArtifactsResponse]:
        job = self._scheduler.get_job(job_uuid)
        return PostTrainingJobArtifactsResponse(job_uuid=job_uuid, checkpoints=self._get_checkpoints(job))
