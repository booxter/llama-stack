#!/usr/bin/env bash

set -ex

export INFERENCE_MODEL=meta-llama/Llama-3.2-3B-Instruct
if [ -e /usr/bin/python3.11 ]; then
  PYTHON=/usr/bin/python3.11
else
  PYTHON=$(which python3.11)
fi
LLS_VENV=train
TEMPLATE=experimental-post-training
CONFIG=~/.llama/distributions/$TEMPLATE/$TEMPLATE-run.yaml

if [ x$1 = x--clean ]; then
  rm -rf venv $LLS_VENV
  $PYTHON -m venv venv
fi
. ./venv/bin/activate
pip install -e .

# Load model if not already
echo test | ollama run llama3.2:3b-instruct-fp16 --keepalive -1m

if ! [ -e ~/.llama/checkpoints/ ]; then
  llama download --source meta --model-id  Llama3.2-3B-Instruct
fi

export UV_PYTHON=$(which python)
llama stack build --template $TEMPLATE --image-type venv --image-name $LLS_VENV

./venv/bin/python -m ensurepip
./venv/bin/pip install kfp==2.12

./$LLS_VENV/bin/python -m ensurepip
./$LLS_VENV/bin/pip3 install kfp==2.12

llama stack run --image-type venv --image-name venv $CONFIG
