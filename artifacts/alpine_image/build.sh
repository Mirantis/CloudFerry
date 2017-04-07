#!/bin/bash

docker build -t build-os-image .
docker run --rm --privileged=true -ti -v "$PWD/result:/result" build-os-image

