#!/bin/bash

# Get all container names matching monster_*
containers=$(docker ps -a --filter "name=monster_*" --format "{{.Names}}")

if [ -z "$containers" ]; then
    echo "No containers found matching pattern 'monster_*'"
    exit 0
fi

echo "Purging logs for containers matching 'monster_*'..."

for container in $containers; do
    # Get the log file path for the container
    log_file=$(docker inspect --format='{{.LogPath}}' "$container" 2>/dev/null)

    if [ -n "$log_file" ] && [ -f "$log_file" ]; then
        echo "Clearing logs for $container: $log_file"
        echo "" > "$log_file"
    else
        echo "Warning: Could not find log file for $container"
    fi
done

echo "Log purge complete!"
