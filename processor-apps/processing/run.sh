#!/bin/bash

set -xe

mvn clean package -DskipTests

java -jar target/flightdeck-streams-0.1.0-SNAPSHOT.jar
