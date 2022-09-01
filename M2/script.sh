#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ssh -n $host nohup "bash -c \"cd $SCRIPT_DIR && java -jar $SCRIPT_DIR/m2-server.jar $name $port $zkPort $ECS_host $isLoadReplica $parentName"\"