#!/bin/bash

curl -H "Content-Type: application/json" --data "{\"source_type\": \"Tag\", \"source_name\": \"$TRAVIS_TAG\"}" -X POST https://registry.hub.docker.com/u/spotify/heroic/trigger/$DOCKER_TOKEN/
