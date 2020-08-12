#!/bin/sh

cd "$(dirname "$0")"

if [ -e pid ]
then
    pid=`cat pid`

    echo "Stopping immudb (pid $pid)..."

    kill -15 "$pid"
    rm pid

    echo "Done."
else
    echo "pid file not found"
fi