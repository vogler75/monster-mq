#!/bin/sh

# Check if PUBLICADDRESS is set and non-empty
if [ -n "$PUBLIC_ADDRESS" ]; then
    HZ_PUBLIC_ADDRESS="-Dhazelcast.local.publicAddress=$PUBLIC_ADDRESS"
else
    HZ_PUBLIC_ADDRESS=""
fi

# List all JAR files and join them with ":"
CLASSPATH=$(ls *.jar 2> /dev/null | tr '\n' ':')

# Remove the trailing ":" if any jars were found
CLASSPATH=$(echo "$CLASSPATH" | sed 's/:$//')

# Run the Java command with the conditional argument
java -XX:+UseNUMA -classpath $CLASSPATH $HZ_PUBLIC_ADDRESS at.rocworks.MainKt config.yaml $@