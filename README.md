<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Benchmark Tool

The pravega benchmark tool used for the performance benchmarking of pravega streaming storage cluster.
This tool performs the throughput and latency analysis for the multi producers and consumers of pravega.
it also validates the end to end latency. The write and/or read latencies can be stored in the CSV file for later analysis.
At the end of the performance benchmarking, this tool outputs the 50th, 75th, 95th , 99th and 99.9th latency percentiles.


### Prerequisites

- Pravega client : https://github.com/pravega/pravega
- Java 8+

### Building

Checkout the source code:

```
git clone https://github.com/pravega/pravega-benchmark
cd pravega-benchmark
```

Build the pravega benchmark Tool:

```
./gradlew build
```

untar the pravega benchmark tool to local folder

```
tar -xvf ./build/distributions/pravega-benchmark.tar -C ./run
```

Running pravega bencmark tool locally:

```
<dir>/pravega-benchmark$ ./run/pravega-benchmark/bin/pravega-benchmark  --help
usage: pravega-benchmark
 -consumers <arg>               number of consumers
 -controller <arg>              controller URI
 -events <arg>                  number of events/records if 'time' not
                                specified;
                                otherwise, maximum events per second by
                                producer(s)and/or number of events per
                                consumer
 -help                          Help message
 -producers <arg>               number of producers
 -readcsv <arg>                 csv file to record read latencies
 -recreate <arg>                If the stream is already existing, delete
                                it and recreate it
 -segments <arg>                Number of segments
 -size <arg>                    Size of each message (event or record)
 -stream <arg>                  Stream name
 -throughput <arg>              if > 0 , throughput in MB/s
                                if 0 , writes 'events'
                                if -1, get the maximum throughput
 -time <arg>                    number of seconds the code runs
 -transaction <arg>             Producers use transactions or not
 -transactionspercommit <arg>   Number of events before a transaction is
                                committed
 -writecsv <arg>                csv file to record write latencies
```

## Running Performance benchmarking

The pravega benchmark tool can be executed to
 - write/read specific amount of events/records to/from the pravega cluster
 - write/read the events/records for the specified amount of time

The pravega benchmark tool can be executed in the following modes:
```
1. Burst Mode
2. Throughput Mode
3. OPS Mode or  Events Rate / Rate limiter Mode
4. End to End Latency Mode
```

### 1 - Burst Mode
In this mode, the pravega benchmark tool pushes/pulls the messages to/from the pravega client as much as possible.
This mode is used to find the maximum and throughput that can be obtained from the pravega cluster.
This mode can be used for both producers and consumers.

```
For example:
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark  --controller tcp://10.240.135.49:9090  --stream streamname1  -segments 1  -producers 1  --size 100   -throughput -1   -time 60

The -throughput -1  indicates the burst mode.
This test will executed for 60 seconds because option -time 60 is used.
This test tries to write and read events of size 100 bytes to/from the stream 'streamname1'.
The option '--controller tcp://10.240.135.49:9090' specifies the pravega controller IP address and port number.
Note that -producers 1 indicates 1 producer/writers.

in case you want to write/read the certain number of events use the --events option without --time option as follows

<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark  --controller tcp://10.240.135.49:9090  --stream streamname1  -segments 1  -producers 1  --size 100   -throughput -1   --events 1000000

--events <number> indicates that total <number> of events to write/read
```

### 2 - Throughput Mode
In this mode, the pravega benchmark tool pushes the messages to the pravega client with specified approximate maximum throughput.
This mode is used to find the least latency  that can be obtained from the pravega cluster for given throughput.
This mode is used only for write operation.

```
For example:
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark  --controller tcp://10.240.135.49:9090  --stream streamname5  -segments 5  -producers 5   --size 100   -throughput 10   -time 300

The -throughput <positive number>  indicates the Throughput mode.

This test will be executed with approximate max throughput of 10MB/sec.
This test will executed for 300 seconds (5 minutes) because option -time 60 is used.
This test tries to write and read events of size 100 bytes to/from the stream 'streamname5' of 5 segments.
If the stream 'streamname5' is not existing , then it will be created with the 5 segments.
if the steam is already existing then it will be scaled up/down to 5 segments.
Note that -producers 5 indicates 5 producers/writers .

in case you want to write/read the certain number of events use the --events option without --time option as follows

<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark  --controller tcp://10.240.135.49:9090  --stream streamname5  -segments 5  -producers 1  --size 100   -throughput 10   --events 1000000

--events 1000000 indicates that total 1000000 (1 million) of events will be written at the throughput speed of 10MB/sec
```

### 3 - OPS Mode or  Events Rate / Rate Limiter Mode
This mode is another form of controlling writers throughput by limiting the number of events per second.
In this mode, the pravega benchmark tool pushes the messages to the pravega client with specified approximate maximum events per sec.
This mode is used to find the least latency  that can be obtained from the pravega cluster for events rate.
This mode is used only for write operation.

```
For example:
<pravega benchmark directory>/run/pravega-benchmark/bin/pravega-benchmark  --controller tcp://10.240.135.49:9090  --stream streamname1  -segments 1  -producers 5  --size 100  -events 1000   -time 60

The -events <event numbers>  (1000 ) specifies the events per second to write.
Note that the option "-throughput"  SHOULD NOT supplied for this OPS Mode or  Events Rate / Rate limiter Mode.

This test will be executed with approximate 1000 events per second by 6 producers.
This test will executed for 300 seconds (5 minutes) because option -time 60 is used.
Note that in this mode, there is 'NO total number of events' to specify hence user must supply the time to run using -time option.
```

### 4 - End to End Latency Mode
In this mode, the pravega benchmark tool writes and read the messages to the pravega cluster and records the end to end latency.
End to end latency means the time duration between the beginning of the writing event/record to stream and the time after reading the event/record.
in this mode user must specify both the number of producers and consumers.
The --throughput option (Throughput mode) or --events (late limiter) can used to limit the writers throughput or events rate.

```
For example:
<pravega benchmark directory>./run/pravega-benchmark/bin/pravega-benchmark  --controller tcp://10.240.135.49:9090  --stream streamname3  -segments 1  -producers 1 -consumers 1  --size 100  -throughput -1   -time 60

The user should specify both producers and consumers count  for write to read or End to End latency mode. it should be set to true.
The -throughput -1 specifies the writes tries to write the events at the maximum possible speed.
```

### Recording the latencies to CSV files
user can use the options "-writecsv  <file name>" to record the latencies of writers and "-readcsv <file name>" for readers.
in case of End to End latency mode, if the user can supply only -readcsv to get the end to end latency in to the csv file.
