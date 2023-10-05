#!/bin/bash
set -e

cd "$(dirname "$0")"

if [ -z "$OSTYPE" ]
then
      OSTYPE="linux-gnu"
fi

if [ ! -e immudb ]
then
  echo "Downloading immudb..."

  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    URL=https://github.com/vchain-us/immudb/releases/download/v1.5.0/immudb-v1.5.0-linux-amd64
    wget -O immudb $URL
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    URL=https://github.com/vchain-us/immudb/releases/download/v1.5.0/immudb-v1.5.0-darwin-amd64
    curl -o immudb -L $URL
  fi

  chmod +x immudb

  echo "Download complete."
fi

echo "Starting immudb..."

./immudb --pidfile pid

