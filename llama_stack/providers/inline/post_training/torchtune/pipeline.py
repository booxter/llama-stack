# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the terms described in the LICENSE file in
# the root directory of this source tree.

from kfp import dsl


def pipeline(
    config,
    data,
    job_uuid,
    training_config,
    hyperparam_search_config,
    logger_config,
    model,
    checkpoint_dir,
    algorithm_config,
):
    @dsl.component
    def component(
        config: dict = {},
        data: list = [],
        job_uuid: str = "",
        training_config: dict = {},
        hyperparam_search_config: dict = {},
        logger_config: dict = {},
        model: str = "",
        checkpoint_dir: str = "",
        algorithm_config: dict = {},
    ):
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
        asyncio.run(recipe.train())

    @dsl.pipeline(name=job_uuid)
    def p(
        config: dict = config,
        data: list = data,
        job_uuid: str = job_uuid,
        training_config: dict = training_config,
        hyperparam_search_config: dict = hyperparam_search_config,
        logger_config: dict = logger_config,
        model: str = model,
        checkpoint_dir: str = checkpoint_dir,
        algorithm_config: dict = algorithm_config,
    ):
        component(
            config=config,
            data=data,
            job_uuid=job_uuid,
            training_config=training_config,
            hyperparam_search_config=hyperparam_search_config,
            logger_config=logger_config,
            model=model,
            checkpoint_dir=checkpoint_dir,
            algorithm_config=algorithm_config,
        )

    return p
