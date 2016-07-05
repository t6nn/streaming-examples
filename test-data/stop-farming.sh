#!/usr/bin/env bash

if [ ! -f current_pids ]; then
    echo "Farming sim not running.";
    exit 1;
fi

while read PID; do
    kill -9 $PID
done < current_pids

rm current_pids