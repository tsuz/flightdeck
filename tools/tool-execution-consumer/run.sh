#!/bin/bash

set -xe

mvn package -DskipTests

MOCK_MODE=true java -jar target/tool-execution-consumer-0.1.0-SNAPSHOT.jar     