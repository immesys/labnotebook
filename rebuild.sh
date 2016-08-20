#!/bin/bash
set -ex
docker build -t immesys/nbfe .
docker push immesys/nbfe
