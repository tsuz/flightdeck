#!/bin/bash

set -xe

mvn package -DskipTests

 java -jar target/chat-api-0.1.0-SNAPSHOT.jar
