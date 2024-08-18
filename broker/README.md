# Build

## Build a single fat JAR

> mvn clean install

## Copy dependencies to a directory

> mvn dependency:copy-dependencies

## Generate a keystore 

> keytool -genkeypair -alias monstermq -keyalg RSA -keysize 2048 -validity 365 -keystore server-keystore.jks -storepass password