/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.perf;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.ClientFactoryImpl;

import java.io.IOException;
import java.time.Instant;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.IntStream;

/**
 * Performance benchmark for Pravega.
 * Data format is in comma separated format as following: {TimeStamp, Sensor Id, Location, TempValue }.
 */
public class PravegaPerfTest {

    private static String controllerUri = "tcp://localhost:9090";
    private static int messageSize = 100;
    private static String streamName = StartLocalService.STREAM_NAME;
    private static String scopeName = StartLocalService.SCOPE;
    private static boolean recreate = false;
    private static int producerCount = 0;
    private static int consumerCount = 0;
    private static int segmentCount = 0;
    private static int events = 3000;
    private static boolean isRandomKey = false;
    private static int transactionPerCommit = 0;
    private static int runtimeSec = 0;
    private static final int reportingInterval = 5000;
    private static ScheduledExecutorService bgexecutor;
    private static ForkJoinPool fjexecutor;
    private static PerfStats produceStats, consumeStats;
    private static double throughput = 0;
    private static ThroughputController tput;
    private static String writeFile = null;
    private static String readFile = null;

    public static void main(String[] args) {
        ReaderGroup readerGroup = null;
        final int timeout = 10;
        final ClientFactory factory;
        ControllerImpl controller = null;

        final List<Callable<Void>> readers;
        final List<Callable<Void>> writers;

        try {
            parseCmdLine(args);
        } catch (ParseException p) {
            p.printStackTrace();
            System.exit(1);
        }
        if (producerCount == 0 && consumerCount == 0) {
            System.out.println("Error: Must specify the number of producers or Consumers");
            System.exit(1);
        }

        bgexecutor = Executors.newScheduledThreadPool(10);
        fjexecutor = new ForkJoinPool();

        try {

            controller = new ControllerImpl(ControllerImplConfig.builder()
                    .clientConfig(ClientConfig.builder()
                            .controllerURI(new URI(controllerUri)).build())
                    .maxBackoffMillis(5000).build(),
                    bgexecutor);

            PravegaStreamHandler streamHandle = new PravegaStreamHandler(scopeName, streamName, controllerUri,
                    segmentCount, timeout, controller,
                    bgexecutor);

            if (producerCount > 0 && !streamHandle.create()) {
                if (recreate) {
                    streamHandle.recreate();
                } else {
                    streamHandle.scale();
                }
            }

            factory = new ClientFactoryImpl(scopeName, controller);
            final Instant StartTime = Instant.now();

            if (consumerCount > 0) {
                readerGroup = streamHandle.createReaderGroup();
                consumeStats = new PerfStats("Reading", reportingInterval, messageSize, consumerCount * events * (runtimeSec + 1), readFile);
                readers = IntStream.range(0, consumerCount)
                        .boxed()
                        .map(i -> new PravegaReaderWorker(i, events,
                                runtimeSec, StartTime, consumeStats,
                                streamName, timeout, factory))
                        .collect(Collectors.toList());
            } else {
                readers = null;
                consumeStats = null;
            }

            if (producerCount > 0) {

                produceStats = new PerfStats("Writing", reportingInterval, messageSize, producerCount * events * (runtimeSec + 1), writeFile);
                if (throughput == 0 && runtimeSec > 0) {
                    tput = new ThroughputController(events);
                } else {
                    tput = new ThroughputController(messageSize, throughput);
                }

                if (transactionPerCommit > 0) {
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new PravegaTransactionWriterWorker(i, events,
                                    runtimeSec, isRandomKey,
                                    messageSize, StartTime,
                                    produceStats, streamName,
                                    tput, factory,
                                    transactionPerCommit))
                            .collect(Collectors.toList());
                } else {
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new PravegaWriterWorker(i, events,
                                    runtimeSec, isRandomKey,
                                    messageSize, StartTime,
                                    produceStats, streamName,
                                    tput, factory))
                            .collect(Collectors.toList());
                }
            } else {
                writers = null;
                produceStats = null;
            }

            final List<Callable<Void>> workers = Stream.of(readers, writers)
                    .filter(x -> x != null)
                    .flatMap(x -> x.stream())
                    .collect(Collectors.toList());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        System.out.println();
                        shutdown();
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            fjexecutor.invokeAll(workers);
            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }

    private static synchronized void shutdown() throws InterruptedException, IOException {
        final Instant endTime = Instant.now();
        if (fjexecutor == null) {
            return;
        }
        fjexecutor.shutdown();
        fjexecutor.awaitTermination(1, TimeUnit.SECONDS);
        fjexecutor = null;
        if (produceStats != null) {
            produceStats.printTotal(endTime);
        }

        if (consumeStats != null) {
            consumeStats.printTotal(endTime);
        }
    }

    private static void parseCmdLine(String[] args) throws ParseException {
        // create Options object
        Options options = new Options();
        final CommandLineParser parser;
        final CommandLine commandline;

        options.addOption("controller", true, "controller URI");
        options.addOption("producers", true, "number of producers");
        options.addOption("consumers", true, "number of consumers");
        options.addOption("events", true,
                "number of events/records per producer/consumer if 'time' not specified;\n" +
                        "otherwise, maximum events per second by producer(s)" +
                        "and/or number of events per consumer");
        options.addOption("time", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message (event or record)");
        options.addOption("stream", true, "Stream name");
        options.addOption("randomkey", true,
                "Set Random key default is one key per producer");
        options.addOption("transactionspercommit", true,
                "Number of events before a transaction is committed");
        options.addOption("segments", true, "Number of segments");
        options.addOption("recreate", true,
                "If the stream is already existing, delete it and recreate it");

        options.addOption("throughput", true,
                "if > 0 , throughput in MB/s\n" +
                        "if 0 , writes 'events'\n" +
                        "if -1, get the maximum throughput");
        options.addOption("writecsv", true, "csv file to record write latencies");
        options.addOption("readcsv", true, "csv file to record read latencies");

        options.addOption("help", false, "Help message");

        parser = new BasicParser();
        commandline = parser.parse(options, args);

        // Since it is command line sample producer, user inputs will be accepted from console
        if (commandline.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("pravega-benchmark", options);
            System.exit(0);
        } else {

            if (commandline.hasOption("controller")) {
                controllerUri = commandline.getOptionValue("controller");
            }
            if (commandline.hasOption("producers")) {
                producerCount = Integer.parseInt(commandline.getOptionValue("producers"));
            }

            if (commandline.hasOption("consumers")) {
                consumerCount = Integer.parseInt(commandline.getOptionValue("consumers"));
            }

            if (commandline.hasOption("events")) {
                events = Integer.parseInt(commandline.getOptionValue("events"));
            }

            if (commandline.hasOption("time")) {
                runtimeSec = Integer.parseInt(commandline.getOptionValue("time"));
            }

            if (commandline.hasOption("size")) {
                messageSize = Integer.parseInt(commandline.getOptionValue("size"));
            }

            if (commandline.hasOption("stream")) {
                streamName = commandline.getOptionValue("stream");
            }

            if (commandline.hasOption("randomkey")) {
                isRandomKey = Boolean.parseBoolean(commandline.getOptionValue("randomkey"));
            }

            if (commandline.hasOption("transactionspercommit")) {
                transactionPerCommit = Integer.parseInt(commandline.getOptionValue("transactionspercommit"));
            }

            if (commandline.hasOption("segments")) {
                segmentCount = Integer.parseInt(commandline.getOptionValue("segments"));
            } else {
                segmentCount = producerCount;
            }

            if (commandline.hasOption("recreate")) {
                recreate = Boolean.parseBoolean(commandline.getOptionValue("recreate"));
            }

            if (commandline.hasOption("throughput")) {
                throughput = Double.parseDouble(commandline.getOptionValue("throughput"));
            }
            if (commandline.hasOption("writecsv")) {
                writeFile = commandline.getOptionValue("writecsv");
            }
            if (commandline.hasOption("readcsv")) {
                readFile = commandline.getOptionValue("readcsv");
            }
        }
    }

    private static class StartLocalService {
        static final int PORT = 9090;
        static final String SCOPE = "Scope";
        static final String STREAM_NAME = "aaj";
    }
}
