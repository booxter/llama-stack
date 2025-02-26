#!/usr/bin/env bash

. ./venv/bin/activate
if ! [ -e ~/.llama/client/config.yaml ]; then
  llama-stack-client configure --endpoint http://localhost:8321
fi
llama-stack-client $@
