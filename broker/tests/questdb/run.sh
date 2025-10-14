#!/bin/bash

# QuestDB Bulk Insert Test Runner
# This script compiles and runs the standalone QuestDB test program

echo "QuestDB Bulk Insert Test"
echo "======================="

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')
if [ "$JAVA_VERSION" -lt 11 ]; then
    echo "Error: Java 11 or higher is required"
    exit 1
fi

echo "Java version: $(java -version 2>&1 | head -n 1)"

# Build and run the QuestDB test program
echo "Building QuestDB test program..."

# Clean and compile with Maven
mvn clean compile

if [ $? -eq 0 ]; then
    echo "Build successful. Running test..."
    
    # Pass all command line arguments to the Java program
    if [ $# -gt 0 ]; then
        echo "Running with arguments: $@"
        mvn exec:java -Dexec.args="$*"
    else
        echo "Running with default settings (localhost:8812/qdb, 10 batches)"
        echo "Usage: $0 [host] [port] [database] [batches]"
        echo "Example: $0 localhost 8812 qdb 20"
        echo "         $0 linux5 8812 qdb 50"
        echo "         $0 192.168.1.100 9999 mydb 100"
        mvn exec:java
    fi
else
    echo "Build failed!"
    exit 1
fi