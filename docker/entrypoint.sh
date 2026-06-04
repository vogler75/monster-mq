#!/bin/sh

# Check if HAZELCAST_CONFIG is set and non-empty
if [ -n "$HAZELCAST_CONFIG" ]; then
    HZ_CONFIG="-Dvertx.hazelcast.config=$HAZELCAST_CONFIG"
else
    HZ_CONFIG=""
fi

# Check if PUBLIC_ADDRESS is set and non-empty
if [ -n "$PUBLIC_ADDRESS" ]; then
    HZ_PUBLIC_ADDRESS="-Dhazelcast.local.publicAddress=$PUBLIC_ADDRESS"
else
    HZ_PUBLIC_ADDRESS=""
fi

# List all JAR files and join them with ":"
CLASSPATH=$(ls *.jar 2> /dev/null | tr '\n' ':')

# Remove the trailing ":" if any jars were found
CLASSPATH=$(echo "$CLASSPATH" | sed 's/:$//')

mkdir -p log

# /cfg-data is the persistent volume mount used by Siemens Industrial Edge
if [ -d /cfg-data ]; then
    mkdir -p /cfg-data/db
    if [ ! -L /app/db ]; then
        rm -rf /app/db
        ln -s /cfg-data/db /app/db
    fi
else
    mkdir -p db
fi

# Ensure config.yaml exists
if [ ! -f /app/config.yaml ] && [ ! -L /app/config.yaml ]; then
    if [ -d /cfg-data ]; then
        if [ ! -f /cfg-data/config.yaml ]; then
            cp /app/config-default.yaml /cfg-data/config.yaml
        fi
        ln -s /cfg-data/config.yaml /app/config.yaml
    else
        cp /app/config-default.yaml /app/config.yaml
    fi
fi

# Detect if Java is GraalVM
JAVA_VERSION=$(java -version 2>&1)
if echo "$JAVA_VERSION" | grep -q "GraalVM"; then
    echo "Detected GraalVM - enabling JVMCI for optimal JavaScript performance"
    JAVA_OPTS="-XX:+EnableJVMCI -XX:+UseJVMCICompiler"
else
    echo "Using standard JVM (GraalVM not detected)"
    JAVA_OPTS=""
fi

JAVA_OPTS="$JAVA_OPTS --enable-native-access=ALL-UNNAMED"


# Run the Java command with the conditional argument
exec java $JAVA_OPTS -classpath $CLASSPATH $HZ_CONFIG $HZ_PUBLIC_ADDRESS at.rocworks.MonsterKt -config config.yaml $@
