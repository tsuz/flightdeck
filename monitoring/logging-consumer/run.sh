#!/bin/bash

set -xe

mvn package -DskipTests

java -jar target/monitoring-consumer-0.1.0-SNAPSHOT.jar      
