#!/bin/bash
set -ex
docker build --no-cache -t immesys/nbfe .
docker push immesys/nbfe
