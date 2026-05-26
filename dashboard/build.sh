#!/bin/bash
set -euo pipefail

# Build dashboard and copy to broker classpath resources
cd "$(dirname "$0")"

if [[ -d ../broker/src/main/resources ]]; then
  broker_resources="../broker/src/main/resources"
elif [[ -d ../main/broker/src/main/resources ]]; then
  broker_resources="../main/broker/src/main/resources"
else
  echo "Could not find broker src/main/resources directory." >&2
  echo "Expected either ../broker/src/main/resources or ../main/broker/src/main/resources." >&2
  exit 1
fi

npm ci
npm run build
rm -rf "$broker_resources/dashboard"
cp -R dist "$broker_resources/dashboard"
rm -f "$broker_resources/dashboard/config/brokers.json"
echo "Dashboard copied to $broker_resources/dashboard"
