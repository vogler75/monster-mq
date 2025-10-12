#!/bin/bash

# MonsterMQ Flow Engine Cleanup Script
# This script deletes all flow instances and flow classes

GRAPHQL_URL="http://localhost:4000/graphql"

echo "=== MonsterMQ Flow Engine Cleanup ==="
echo ""

# Check if server is running
echo "Checking if GraphQL server is running..."
if ! curl -s -m 5 http://localhost:4000/health > /dev/null 2>&1; then
  echo "✗ Error: GraphQL server is not responding at $GRAPHQL_URL"
  echo "  Please start the MonsterMQ broker first"
  exit 1
fi
echo "✓ Server is running"
echo ""

# Step 1: Get all flow instances
echo "Fetching all flow instances..."
INSTANCES=$(curl -s -X POST $GRAPHQL_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { flowInstances { name } }"
  }' | jq -r '.data.flowInstances[].name')

# Step 2: Delete all flow instances
if [ -z "$INSTANCES" ]; then
  echo "No flow instances found."
else
  echo "Found flow instances:"
  echo "$INSTANCES"
  echo ""
  echo "Deleting flow instances..."

  while IFS= read -r instance; do
    if [ -n "$instance" ]; then
      echo "  - Deleting instance: $instance"
      curl -s -X POST $GRAPHQL_URL \
        -H "Content-Type: application/json" \
        -d "{
          \"query\": \"mutation { flow { deleteInstance(name: \\\"$instance\\\") } }\"
        }" | jq -r '.data.flow.deleteInstance'
    fi
  done <<< "$INSTANCES"
fi

echo ""

# Step 3: Get all flow classes
echo "Fetching all flow classes..."
CLASSES=$(curl -s -X POST $GRAPHQL_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { flowClasses { name } }"
  }' | jq -r '.data.flowClasses[].name')

# Step 4: Delete all flow classes
if [ -z "$CLASSES" ]; then
  echo "No flow classes found."
else
  echo "Found flow classes:"
  echo "$CLASSES"
  echo ""
  echo "Deleting flow classes..."

  while IFS= read -r class; do
    if [ -n "$class" ]; then
      echo "  - Deleting class: $class"
      curl -s -X POST $GRAPHQL_URL \
        -H "Content-Type: application/json" \
        -d "{
          \"query\": \"mutation { flow { deleteClass(name: \\\"$class\\\") } }\"
        }" | jq -r '.data.flow.deleteClass'
    fi
  done <<< "$CLASSES"
fi

echo ""
echo "=== Cleanup Complete ==="
