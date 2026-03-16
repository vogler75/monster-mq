#!/bin/bash
# Build dashboard and copy to broker classpath resources
cd "$(dirname "$0")"
npm ci
npm run build
rm -rf ../broker/src/main/resources/dashboard
cp -r dist ../broker/src/main/resources/dashboard
rm -f ../broker/src/main/resources/dashboard/config/brokers.json
echo "Dashboard copied to broker/src/main/resources/dashboard"
