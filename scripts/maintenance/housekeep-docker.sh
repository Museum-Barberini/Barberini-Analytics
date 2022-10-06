#!/bin/bash
# NOTE: Some of the following might be required only as a workaround for stale luigi schedulers: https://github.com/spotify/luigi/issues/2070

docker rm $(docker ps -qaf status=exited)
docker rmi $(docker images -aq)

# Maybe try these:
docker kill $(docker ps -qaf name=gitlab_runner)
docker rm $(docker ps -qaf name=gitlab_runner)
docker rmi -f $(docker images -aq) # not proven
docker rmi -f $(docker images -f "dangling=true" -q) # not proven
docker rmi $(docker images --format '{{.Repository}}:{{.Tag}}' | grep gitlab_runner)
