#!/bin/bash

set -eu

./gradlew assemble

scp build/libs/kinesis-0.0.1-SNAPSHOT.jar ec2-user@ec2-3-67-80-88.eu-central-1.compute.amazonaws.com:kinesis-0.0.1-SNAPSHOT.jar
scp build/libs/kinesis-0.0.1-SNAPSHOT.jar ec2-user@ec2-35-158-106-1.eu-central-1.compute.amazonaws.com:kinesis-0.0.1-SNAPSHOT.jar
scp build/libs/kinesis-0.0.1-SNAPSHOT.jar ec2-user@ec2-3-122-41-239.eu-central-1.compute.amazonaws.com:kinesis-0.0.1-SNAPSHOT.jar
