#!/bin/bash

set -x

RaftLogDir="/var/log/raft_logs"
RaftLogFile="all_servers.log"

echo "waiting for docker compose to run"
docker compose up --build &> $RaftLogDir$"/"$RaftLogFile

# docker compose down
# echo "stopped containers"

server_prefix="raft-server-"
# Don't mofify this. This seems to be added by docker compose 
server_suffix="-1"

n=5
for ((i=0; i<n; i++))
do
    container_name=$server_prefix$i$server_suffix
    echo "writing logs for $container_name"
    container_log_path=$RaftLogDir$"/"$i".log"
    sed -n "/^$container_name /w $container_log_path" $RaftLogDir$"/"$RaftLogFile
done

echo "extracted logs into separate server files"