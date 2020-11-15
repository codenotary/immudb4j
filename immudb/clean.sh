#!/bin/bash

cd "$(dirname "$0")" || exit

if [ -e pid ]
then
  sh ./stop.sh

  # give some time to immudb to gracefully shutdown
  sleep 1
fi

rm -rf data
rm -rf roots

echo "immudb4j data and roots folders removed."