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
from llama_stack.apis.datatypes import Api
from llama_stack.distribution.distribution import get_provider_registry

from .config import TorchtunePostTrainingConfig


def _get_provider_pip_dependencies(api_type: Api, provider_name: str| None = None) -> list[str]:
    deps = [
        # TODO: how to achieve identical llama-stack code on both sides?
        "git+https://github.com/booxter/llama-stack.git@kflow#egg=llama-stack",
    ] # always install the base package
    provider_registry = get_provider_registry()
    for name, spec in provider_registry[api_type].items():
        if provider_name is None or name == provider_name:
            deps += spec.pip_packages

    # drop unnecessary dependencies
    # TODO: make it more generic / separate scheduler deps?
    deps.remove("kfp")
    deps.remove("kubernetes")

    return deps


# TODO: we should probably have a container image with all dependencies pre-built
_BASE_IMAGE = "quay.io/fedora/python-311:311"


def lls_component(api_type: Api, provider_name: str| None = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return dsl.component(
                base_image=_BASE_IMAGE,
                func=func,
                packages_to_install=_get_provider_pip_dependencies(api_type, provider_name)
            )(*args, **kwargs)
        return wrapper
    return decorator


@lls_component(Api.post_training, "inline::torchtune")
def component(
    config: dict,
    data: list, # should be an Input?
    job_uuid: str,
    training_config: dict,
    hyperparam_search_config: dict,
    logger_config: dict,
    model: str,
    checkpoint_dir: str,
    model_artifact: Artifact,
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

    # Extract checkpoint from passed artifact
    import os
    import tarfile
    model_dir = os.path.dirname(model_artifact.path)

    dest_dir = os.path.join(
        model_dir,
        # This is the first part of the model/name - TODO: clunky and will have to be dealt more gracefully
        os.path.dirname(model)
    )
    # TODO: Though named .gz, the file is actually a tar archive (muh bad!)
    with tarfile.open(model_artifact.path) as tar:
        tar.extractall(path=dest_dir)

    # Ignore passed checkpoint value
    checkpoint_dir = os.path.join(model_dir, model)

    os.system(f"find {checkpoint_dir}")

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

    a = Artifact(
        uri=dsl.get_uri(),
        metadata={
            'resources_allocated': resources_allocated,
            'checkpoints': [],
        }
    )

    # Copy checkpoint files to pipeline artifacts
    import shutil
    for checkpoint in checkpoints:
        chk = checkpoint.model_copy()
        chk.path = f"{a.path}/{checkpoint.identifier}"
        a.metadata['checkpoints'].append(_serialize(chk))
        # TODO: handle any errors
        shutil.copytree(checkpoint.path, chk.path)

    return a


# TODO: should serialize use strings to pass models between components?
def _serialize(obj: BaseModel) -> dict:
    return obj.model_dump(exclude_none=True, mode="json")


# TODO: it would be nice if we could pass pydantic models transparently between
# components (with serialization and deserialization offloaded to kfp
# machinery): https://github.com/kubeflow/pipelines/issues/10690
def pipeline(
    config: TorchtunePostTrainingConfig,
    data: list[dict],
    job_uuid: str,
    training_config: TrainingConfig,
    hyperparam_search_config: dict,
    logger_config: dict,
    model: str,
    checkpoint_dir: str, # TODO: remove the input argument
    algorithm_config: LoraFinetuningConfig,
):
    # TODO: pass it through artifact to avoid issues with size
    data = data[:10]

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
        importer_task = dsl.importer(
            artifact_uri='s3://rhods-dsp-dev/llama3.2-3b-instruct.tar.gz',
            artifact_class=dsl.Dataset,
        )

        return component(
            config=config,
            data=data,
            job_uuid=job_uuid,
            training_config=training_config,
            hyperparam_search_config=hyperparam_search_config,
            logger_config=logger_config,
            model=model,
            checkpoint_dir=checkpoint_dir,
            model_artifact=importer_task.output,
            algorithm_config=algorithm_config,
        ).output

    return p
