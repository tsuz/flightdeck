#!/bin/bash

set -xe

mvn clean package -DskipTests

java -jar target/axon-streams-0.1.0-SNAPSHOT.jar
