#!/usr/bin/env bash

cd dist
spark-submit --py-files app.zip app.py --env dev