#!/bin/bash

# Check if at least one port argument is provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <port1> <port2> ... <port10>"
    exit 1
fi
sudo systemctl stop redis-server
for port in "$@"; do
    echo "Starting Redis server on port $port..."
    fuser -k "$port"/tcp
    pid=$(sudo lsof -t -i :"$port")
    echo "$pid"
    kill -9 "$pid"
    cmd="redis-server --port $port --dbfilename raft$port.rdb --loadmodule /home/baadalvm/redisraft/redisraft.so --raft.log-filename raftlog$port.db --raft.addr localhost:$port"

    echo "$cmd" > "temp_script_$port.sh"

    chmod +x "temp_script_$port.sh"

    ./temp_script_"$port".sh &
done

firstport="$1"
redis-cli -p "$firstport" raft.cluster init
echo "redis-cli -p $firstport raft.cluster init"
for (( i=2 ; i<=$# ; i++ ));
do
	port="${!i}"
	redis-cli -p "$port" RAFT.CLUSTER JOIN localhost:"$firstport"
done
