<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Benchmark Tool

The Pravega benchmark tool used for the performance benchmarking of pravega streaming storage cluster.
This tool performs the throughput and latency analysis for the multi producers/writers and consumers/readers of pravega.
it also validates the end to end latency. The write and/or read latencies can be stored in a CSV file for later analysis.
At the end of the performance benchmarking, this tool outputs the 50th, 75th, 95th , 99th, 99.9th and 99.99th latency percentiles.


### Prerequisites

- Java 8+

### Building

Checkout the source code:

```
git clone https://github.com/pravega/pravega-benchmark
cd pravega-benchmark
```

Build the Pravega benchmark Tool:

```
./gradlew build
```

untar the Pravega benchmark tool to local folder

```
tar -xvf ./build/distributions/pravega-benchmark.tar -C ./run
```

Running Pravega benchmark tool locally:

```
<dir>/pravega-benchmark$ ./run/pravega-benchmark/bin/pravega-benchmark  -help
usage: pravega-benchmark
 -consumers <arg>                    Number of consumers
 -controller <arg>                   Controller URI
 -enableConnectionPooling <arg>      Set to true to enable connection
                                     pooling
 -events <arg>                       Number of events/records if 'time'
                                     not specified;
                                     otherwise, Maximum events per second
                                     by producer(s) and/or Number of
                                     events per consumer
 -flush <arg>                        Each producer calls flush after
                                     writing <arg> number of of
                                     events/records; Not applicable, if
                                     both producers and consumers are
                                     specified
 -help                               Help message
 -producers <arg>                    Number of producers
 -readcsv <arg>                      CSV file to record read latencies
 -readWatermarkPeriodMillis <arg>    If -1 (default), watermarks will not
                                     be read.
                                     If >0, watermarks will be read with a
                                     period of this many milliseconds.
 -recreate <arg>                     If the stream is already existing,
                                     delete and recreate the same
 -scope <arg>                        Scope name
 -segments <arg>                     Number of segments
 -size <arg>                         Size of each message (event or
                                     record)
 -stream <arg>                       Stream name
 -throughput <arg>                   if > 0 , throughput in MB/s
                                     if 0 , writes 'events'
                                     if -1, get the maximum throughput
 -time <arg>                         Number of seconds the code runs
 -transactionspercommit <arg>        Number of events before a transaction
                                     is committed
 -writecsv <arg>                     CSV file to record write latencies
 -writeWatermarkPeriodMillis <arg>   If -1 (default), watermarks will not
                                     be written.
                                     If 0 and not using transactions,
                                     watermarks will be written after
                                     every event.
                                     If >0 and not using transactions,
                                     watermarks will be written with a
                                     period of this many milliseconds.
                                     If >= 0 and using transactions,
                                     watermarks will be written on each
                                     commit.
 -validateCertHostName               Whether to turn on host name verification
                                     for TLS certificates
```
## Running Performance benchmarking

The Pravega benchmark tool can be executed to
 - write/read specific amount of events/records to/from the Pravega cluster
 - write/read the events/records for the specified amount of time

The Pravega benchmark tool can be executed in the following modes:

1. Burst Mode
2. Throughput Mode
3. OPS Mode or  Events Rate / Rate limiter Mode
4. End to End Latency Mode

### 1 - Burst Mode
In this mode, the Pravega benchmark tool pushes/pulls the messages to/from the Pravega client as much as possible.
This mode is used to find the maximum and throughput that can be obtained from the Pravega cluster.
This mode can be used for both producers and consumers.

For example:
```bash
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark  \
    -controller tcp://127.0.0.1:9090  \
    -stream streamname1 \ 
    -segments 1 \
    -producers 1 \
    -size 100 \
    -throughput -1  \ 
    -time 60
```
The `-throughput -1`  indicates the burst mode.
This test will executed for 60 seconds because option `-time 60` is used.
This test tries to write and read events of size 100 bytes to/from the stream 'streamname1'.
The option `-controller tcp://127.0.0.1:9090` specifies the pravega controller IP address and port number.
Note that -producers 1 indicates 1 producer/writers.

in the case you want to write/read the certain number of events use the `-events` option without `-time` option as follows

```bash
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark \
    -controller tcp://127.0.0.1:9090 \
    -stream streamname1 \
    -segments 1 \
    -producers 1 \
    -size 100 \
    -throughput -1 \ 
    -events 1000000
```
`-events <number>` indicates that total <number> of events to write/read

### 2 - Throughput Mode
In this mode, the Pravega benchmark tool pushes the messages to the Pravega client with specified approximate maximum throughput in terms of Mega Bytes/second (MB/s).
This mode is used to find the least latency that can be obtained from the Pravega cluster for given throughput.
This mode is used only for write operation.

For example:
```bash
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark \
    -controller tcp://127.0.0.1:9090 \
    -stream streamname5 \
    -segments 5 \
    -producers 5 \  
    -size 100 \
    -throughput 10 \
    -time 300
```
The `-throughput <positive number>`  indicates the Throughput mode.

This test will be executed with approximate max throughput of 10MB/sec.
This test will executed for 300 seconds (5 minutes) because option `-time 60` is used.
This test tries to write and read events of size 100 bytes to/from the stream 'streamname5' of 5 segments.
If the stream 'streamname5' is not existing , then it will be created with the 5 segments.
if the steam is already existing then it will be scaled up/down to 5 segments.
Note that `-producers 5` indicates 5 producers/writers .

in the case you want to write/read the certain number of events use the `-events` option without `-time` option as follows:

```bash
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark \
    -controller tcp://127.0.0.1:9090 \
    -stream streamname5 \
    -segments 5 \
    -producers 1 \
    -size 100 \
    -throughput 10 \
    -events 1000000
```
`-events 1000000` indicates that total 1000000 (1 million) of events will be written at the throughput speed of 10MB/sec

### 3 - OPS Mode or  Events Rate / Rate Limiter Mode
This mode is another form of controlling writers throughput by limiting the number of events per second.
In this mode, the Pravega benchmark tool pushes the messages to the Pravega client with specified approximate maximum events per sec.
This mode is used to find the least latency  that can be obtained from the Pravega cluster for events rate.
This mode is used only for write operation.

For example:
```bash
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark \
    -controller tcp://127.0.0.1:9090 \
    -stream streamname1 \
    -segments 1 \
    -producers 5 \
    -size 100 \
    -events 1000 \
    -time 60
```
The `-events <event numbers>`  (1000 ) specifies the events per second to write.
Note that the option `-throughput`  SHOULD NOT supplied for this OPS Mode or  Events Rate / Rate limiter Mode.

This test will be executed with approximate 1000 events per second by 6 producers.
This test will executed for 300 seconds (5 minutes) because option `-time 60` is used.
Note that in this mode, there is 'NO total number of events' to specify hence user must supply the time to run using -time option.

### 4 - End to End Latency Mode
In this mode, the Pravega benchmark tool writes and read the messages to the Pravega cluster and records the end to end latency.
End to end latency means the time duration between the beginning of the writing event/record to stream and the time after reading the event/record.
in this mode user must specify both the number of producers and consumers.
The -throughput option (Throughput mode) or -events (late limiter) can used to limit the writers throughput or events rate.

For example:
```bash
<pravega benchmark directory>./run/pravega-benchmark/bin/pravega-benchmark \
    -controller tcp://127.0.0.1:9090  \
    -stream streamname3  \
    -segments 1  \
    -producers 1 -consumers 1  \
    -size 100 \
    -throughput -1 \
    -time 60
```
The user should specify both producers and consumers count  for write to read or End to End latency mode. it should be set to true.
The `-throughput -1` specifies the writes tries to write the events at the maximum possible speed.

### Recording the latencies to CSV files
User can use the options `-writecsv  <file name>` to record the latencies of writers and `-readcsv <file name>` for readers.
in case of End to End latency mode, if the user can supply only `-readcsv` to get the end to end latency in to the csv file.

## Configuring Security Parameters

To run against Security Enabled Pravega, you will need to specify TLS and authentication/authorization parameters
for the client via the `JAVA_TOOL_OPTIONS` environment variable, before executing this tool.

Below is an example:

```bash
JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/path/to/client.truststore.jks"
JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dpravega.client.auth.method=Basic"
JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dpravega.client.auth.token=YWRtaW46MTExMV9hYWFh"
export JAVA_TOOL_OPTIONS
```

Note:
* The specified token `YWRtaW46MTExMV9hYWFh` shown in the example above equals Base64 encoded value of credentials
in `{username}:{password}` format supported by the specified authentication method `Basic`. The specified token is
Base64 encoded value of string `admin:1111_aaaa`, represent the admin account available in the Password Auth Handler
database. If you are using another authentication method supported by a custom Pravega Auth Handler, generate a
corresponding token, and specify the token and method here instead.
* TLS host name verification is turned off by default to make it easier to run this tool. To enable hostname
verification, specify `-validateCertHostName true` option when executing it.

## Running in Docker

Build and push the Docker image to your repo.
```bash
export DOCKER_REPOSITORY=<your Docker user name>
export IMAGE_TAG=latest
scripts/build-docker.sh
```

Sample command to run in Docker:
```bash
docker run --rm -it \
--network host \
${DOCKER_REPOSITORY}/pravega-benchmark:${IMAGE_TAG} \
-controller tcp://localhost:9090 \
-stream benchmark1 -producers 1 -size 100 -throughput 0.01 -time 15
```

## Running in Kubernetes

You must first build and push the Docker image as described above.

Edit the file scripts/pravega-benchmark.yaml.
You must change the image value to match the DOCKER_REPOSITORY and IMAGE_TAG you used
when built the Docker image.
You must change the Pravega controller URI.
You may also set other parameters as desired.

```bash
export NAMESPACE=examples
scripts/pravega-benchmark-k8s.sh.
```
