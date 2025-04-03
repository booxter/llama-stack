# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.

from kfp import dsl
from kfp.dsl import Artifact
from pydantic import BaseModel

from llama_stack.apis.post_training import (
    LoraFinetuningConfig,
    TrainingConfig,
)
from .config import TorchtunePostTrainingConfig


@dsl.component
def component(
    config: dict,
    data: list, # should be an Input?
    job_uuid: str,
    training_config: dict,
    hyperparam_search_config: dict,
    logger_config: dict,
    model: str,
    checkpoint_dir: str,
    algorithm_config: dict,
) -> Artifact:
    from llama_stack.apis.post_training import (
        LoraFinetuningConfig,
        TrainingConfig,
    )
    from llama_stack.providers.inline.post_training.torchtune.config import (
        TorchtunePostTrainingConfig,
    )
    from llama_stack.providers.inline.post_training.torchtune.recipes.lora_finetuning_single_device import (
        LoraFinetuningSingleDevice,
    )

    recipe = LoraFinetuningSingleDevice(
        TorchtunePostTrainingConfig(**config),
        job_uuid,
        TrainingConfig(**training_config),
        hyperparam_search_config,
        logger_config,
        model,
        checkpoint_dir,
        LoraFinetuningConfig(**algorithm_config),
        data=data,
    )

    import asyncio
    asyncio.run(recipe.setup())
    resources_allocated, checkpoints = asyncio.run(recipe.train())

    # TODO: how does one reuse code with kfp?
    def _serialize(obj) -> dict:
        return obj.model_dump(exclude_none=True, mode="json")

    return Artifact(
        uri=checkpoints[-1].path,
        metadata={
            'resources_allocated': resources_allocated,
            'checkpoints': [_serialize(checkpoint) for checkpoint in checkpoints],
        }
    )


# TODO: should serialize use strings to pass models between components?
def _serialize(obj: BaseModel) -> dict:
    return obj.model_dump(exclude_none=True, mode="json")


def pipeline(
    config: TorchtunePostTrainingConfig,
    data: list[dict],
    job_uuid: str,
    training_config: TrainingConfig,
    hyperparam_search_config: dict,
    logger_config: dict,
    model: str,
    checkpoint_dir: str,
    algorithm_config: LoraFinetuningConfig,
):
    @dsl.pipeline(name=job_uuid)
    def p(
        config: dict = _serialize(config),
        data: list = data,
        job_uuid: str = job_uuid,
        training_config: dict = _serialize(training_config),
        hyperparam_search_config: dict = hyperparam_search_config,
        logger_config: dict = logger_config,
        model: str = model,
        checkpoint_dir: str = checkpoint_dir,
        algorithm_config: dict = _serialize(algorithm_config),
    ) -> Artifact:
        return component(
            config=config,
            data=data,
            job_uuid=job_uuid,
            training_config=training_config,
            hyperparam_search_config=hyperparam_search_config,
            logger_config=logger_config,
            model=model,
            checkpoint_dir=checkpoint_dir,
            algorithm_config=algorithm_config,
        ).output

    return p
