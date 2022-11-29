#!/bin/sh

cd "$(dirname "$0")" || exit

if [ -e pid ]
then
  sh ./stop.sh

  # Giving some time to immudb to gracefully shutdown.
  sleep 2
fi

rm -rf data

echo "immudb's data folder was removed."

