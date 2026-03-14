#!/bin/bash

set -xe

mvn package -DskipTests

CLAUDE_API_KEY=$1 java -jar target/think-consumer-0.1.0-SNAPSHOT.jar