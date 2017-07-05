#!/bin/bash

python ./src/flagged_purchases.py \
--batch-file ./log_input/batch_log.json \
--stream-file ./log_input/stream_log.json \
--flag-file ./log_output/flagged_purchases.json \
--invalid-file ./log_output/invalid_entries.txt \
--std-threshold 3

python ./src/unittests.py
