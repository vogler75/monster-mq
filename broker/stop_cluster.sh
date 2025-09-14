#!/bin/bash

echo "Stopping all MonsterKt instances..."

# Find and kill all java processes running MonsterKt
pkill -f "at.rocworks.MonsterKt"

# Wait a moment for graceful shutdown
sleep 2

# Force kill any remaining processes if they didn't stop gracefully
pkill -9 -f "at.rocworks.MonsterKt" 2>/dev/null

echo "All MonsterKt instances stopped."