# Kinesis Throughput Test

This program can be used to test (aka play around) with Kinesis and its throughput.

1.) Create two Kinesis stream with 1 shard:

* inbound
* outbound

2.) Create an EC2 instance and install JDK 11 via

```
amazon-linux-extras install java-openjdk11
```

3.) Assemble this program and copy it to the EC2 instance

```
./gradlew assemble
scp build/libs/kinesis-0.0.1-SNAPSHOT.jar ec2-user@$HOST_NAME:kinesis-0.0.1-SNAPSHOT.jar
```

4.) Create a start.sh

```
#!/bin/bash

killall java
nohup java -jar -Dspring.profiles.active=transceiver kinesis-0.0.1-SNAPSHOT.jar > transceiver.log 2>&1 &
nohup java -jar -Dspring.profiles.active=messagehandler kinesis-0.0.1-SNAPSHOT.jar > messagehandler.log 2>&1 &
```

5.) Create ~/.aws/credentials and put credentials for [sy] into it

6.) start with `./start.sh`
