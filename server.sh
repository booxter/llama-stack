#!/usr/bin/env bash

set -ex

export INFERENCE_MODEL=meta-llama/Llama-3.2-3B-Instruct
if [ -e /usr/bin/python3.11 ]; then
  PYTHON=/usr/bin/python3.11
else
  PYTHON=$(which python3.11)
fi
TEMPLATE=experimental-post-training
CONFIG=~/.llama/distributions/$TEMPLATE/$TEMPLATE-run.yaml

if [ x$1 = x--clean -o ! -d venv ]; then
  rm -rf venv
  $PYTHON -m venv venv
fi
. ./venv/bin/activate
pip install -e .
pip install uv

# Load model if not already
echo test | ollama run llama3.2:3b-instruct-fp16 --keepalive -1m

if ! [ -e ~/.llama/checkpoints/ ]; then
  llama download --source meta --model-id Llama3.2-3B-Instruct
fi

llama stack build --image-type venv --run --template $TEMPLATE
