#!/bin/bash

set -xe

mvn package -DskipTests

CLAUDE_API_KEY=$1 java -jar target/update-memoir-consumer-0.1.0-SNAPSHOT.jar
