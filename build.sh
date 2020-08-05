#!/usr/bin/env bash

mkdir ./dist
cd dist
mkdir config
mkdir log
cd ..

cp ./app.py ./dist
cp ./config/*.json ./dist/config
zip -r ./dist/app.zip ./config ./core ./jars ./jobs ./libs ./log
