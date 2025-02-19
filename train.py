import os
import time
import uuid


def create_http_client():
    from llama_stack_client import LlamaStackClient

    return LlamaStackClient(
        base_url=f"http://localhost:{os.environ['LLAMA_STACK_PORT']}"
    )

client = (
    create_http_client()
)

# List available models
models = client.models.list()
print("--- Available models: ---")
for m in models:
    print(f"- {m.identifier}")
print()

#response = client.inference.chat_completion(
#    model_id=os.environ["INFERENCE_MODEL"],
#    messages=[
#        {"role": "system", "content": "You are a helpful assistant."},
#        {"role": "user", "content": "Write a haiku about coding"},
#    ],
#)
#print(response.completion_message.content)


simpleqa_dataset_id = "huggingface::simpleqa1"
_ = client.datasets.register(
    dataset_id=simpleqa_dataset_id,
    provider_id="huggingface-0",
    url={"uri": "https://huggingface.co/datasets/llamastack/evals"},
    metadata={
        "path": "llamastack/evals",
        "name": "evals__simpleqa",
        "split": "train",
    },
    dataset_schema={
        #"input_query": {"type": "string"},
        "expected_answer": {"type": "string"},
        "chat_completion_input": {"type": "chat_completion_input"},
    },
)


from llama_stack_client.types import (
    post_training_supervised_fine_tune_params,
)

training_config = post_training_supervised_fine_tune_params.TrainingConfig(
    data_config=post_training_supervised_fine_tune_params.TrainingConfigDataConfig(
        batch_size=1,
        data_format="instruct",
        #data_format="dialog",
        dataset_id=simpleqa_dataset_id,
        shuffle=True,
    ),
    gradient_accumulation_steps=1,
    max_steps_per_epoch=1,
    max_validation_steps=1,
    n_epochs=1,
    optimizer_config=post_training_supervised_fine_tune_params.TrainingConfigOptimizerConfig(
        lr=2e-5,
        num_warmup_steps=1,
        optimizer_type="adam",
        weight_decay=0.01,
    ),
)

from llama_stack_client.types import (
    algorithm_config_param,
)

algorithm_config = algorithm_config_param.LoraFinetuningConfig(
    alpha=1,
    apply_lora_to_mlp=True,
    # Invalid value: apply_lora_to_output is currently not supporting in llama3.2 1b and 3b,as the projection layer weights are tied to the embeddings
    apply_lora_to_output=False,
    lora_attn_modules=['q_proj'], # todo?
    rank=1,
    type="LoRA",
)

job_uuid = f'test-job{uuid.uuid4()}'
training_model = os.environ["INFERENCE_MODEL"]

start_time = time.time()
response = client.post_training.supervised_fine_tune(
    job_uuid=job_uuid,
    logger_config={},
    model=training_model,
    hyperparam_search_config={},
    training_config=training_config,
    algorithm_config=algorithm_config,
    checkpoint_dir="null", # API claims it's not needed but - 400 if not passed.
)

print("Job: ", job_uuid)

while True:
    status = client.post_training.job.status(job_uuid=job_uuid)
    if not status:
        print("Job not found")
        break

    print(status)
    if status.status == "completed":
        break

    print("Waiting for job to complete...")
    time.sleep(30)

end_time = time.time()
print("Job completed in", end_time - start_time, "seconds!")
