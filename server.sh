#!/usr/bin/env bash

set -ex

export INFERENCE_MODEL=meta-llama/Llama-3.2-3B-Instruct
PYTHON=/usr/bin/python3.11
LLS_VENV=train
TEMPLATE=experimental-post-training
CONFIG=~/.llama/distributions/$TEMPLATE/$TEMPLATE-run.yaml

if [ x$1 = x--clean ]; then
  rm -rf venv $LLS_VENV
  $PYTHON -m venv venv
  . ./venv/bin/activate
  pip install -e .
fi

# Load model if not already
echo test | ollama run llama3.2:3b-instruct-fp16 --keepalive -1m

if ! [ -e ~/.llama/checkpoints/ ]; then
  llama download --source meta --model-id  Llama3.2-3B-Instruct
fi

export UV_PYTHON=$(which python)
llama stack build --template $TEMPLATE --image-type venv --image-name $LLS_VENV
llama stack run --image-type venv --image-name venv $CONFIG

