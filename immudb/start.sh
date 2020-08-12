#!/bin/sh

cd "$(dirname "$0")"

if [ ! -e immudb ]
then
  echo "Downloading immudb..."

  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    URL=https://github.com/vchain-us/immudb/releases/download/v0.7.0/immudb-v0.7.0-linux-amd64
    wget -O immudb $URL
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    URL=https://github.com/vchain-us/immudb/releases/download/v0.7.0/immudb-v0.7.0-darwin-amd64
    curl -o immudb -L $URL
  fi

  chmod +x immudb

  echo "Downloaded"
fi

echo "Starting immudb..."

rm -rf data
./immudb --pidfile pid -d

sleep 1

echo "Done."