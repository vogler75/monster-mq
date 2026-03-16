#!/bin/bash
# Build dashboard and copy to broker classpath resources
cd "$(dirname "$0")"
npm ci
npm run build
rm -rf ../broker/src/main/resources/dashboard
cp -r dist ../broker/src/main/resources/dashboard
echo "Dashboard copied to broker/src/main/resources/dashboard"
