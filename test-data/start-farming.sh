#!/usr/bin/env bash

if [ "$#" -ne 3 ]; then
    echo "Usage: start-farming.sh directory number_of_farmers first_port";
    exit 1;
fi

if [ -f current_pids ]; then
    echo "Farming sim already running. Stop with stop-farming.sh";
    exit 1;
fi

DIR=$1
NUM_FARMERS=$2
FIRST_PORT=$3

touch $DIR/counter.log
tail -f $DIR/counter.log | nc -ln $FIRST_PORT &
echo $! >> current_pids

for n in `seq 1 $NUM_FARMERS`
do
    touch $DIR/farmer$n.log
    tail -f $DIR/farmer$n.log | nc -ln $((FIRST_PORT+n)) &
    echo $! >> current_pids
done