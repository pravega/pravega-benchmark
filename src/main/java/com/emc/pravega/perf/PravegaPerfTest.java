/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;

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
    private static int eventsPerWorker = 40;
    private static boolean isTransaction = false;
    private static int reportingInterval = 1000;
    private static boolean runKafka = false;
    private static boolean isRandomKey = false;
    private static int transactionPerCommit = 1;
    private static int runtimeSec = (60 * 60 * 24);

    public static void main(String[] args) {

        Instant endTime;
        ReaderGroup readerGroup = null;
        final int timeout = 10;
        final ClientFactory factory;
        ControllerImpl controller = null;
        ScheduledExecutorService bgexecutor;
        ForkJoinPool fjexecutor;
        final PerfStats produceStats, consumeStats, drainStats;
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
                drainStats = new PerfStats("Draining", reportingInterval, messageSize);
                consumeStats = new PerfStats("Reading", reportingInterval, messageSize);

                readers = IntStream.range(0, consumerCount)
                                   .boxed()
                                   .map(i -> new PravegaReaderWorker(i, eventsPerWorker,
                                           runtimeSec, StartTime, consumeStats,
                                           streamName, timeout, factory))
                                   .collect(Collectors.toList());

                if (producerCount > 0) {
                    PravegaReaderWorker r = (PravegaReaderWorker) readers.get(0);
                    r.cleanupEvents(drainStats);
                }
            } else {
                readers = null;
                drainStats = null;
                consumeStats = null;
            }

            if (producerCount > 0) {

                produceStats = new PerfStats("Writing", reportingInterval, messageSize);
                if (isTransaction) {

                    writers = IntStream.range(0, producerCount)
                                       .boxed()
                                       .map(i -> new PravegaTransactionWriterWorker(i, eventsPerWorker,
                                               runtimeSec, isRandomKey,
                                               messageSize, StartTime,
                                               produceStats, streamName,
                                               factory, transactionPerCommit))
                                       .collect(Collectors.toList());
                } else {

                    writers = IntStream.range(0, producerCount)
                                       .boxed()
                                       .map(i -> new PravegaWriterWorker(i, eventsPerWorker,
                                               runtimeSec, isRandomKey,
                                               messageSize, StartTime,
                                               produceStats, streamName,
                                               factory))
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
            fjexecutor.invokeAll(workers);
            fjexecutor.shutdown();
            fjexecutor.awaitTermination(runtimeSec, TimeUnit.SECONDS);
            endTime = Instant.now();
            if (produceStats != null) {
                produceStats.printAll();
                produceStats.printTotal(endTime);
            }

            if (consumeStats != null) {
                consumeStats.printTotal(endTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }

    private static void parseCmdLine(String[] args) throws ParseException {
        // create Options object
        Options options = new Options();
        final CommandLineParser parser;
        final CommandLine commandline;

        options.addOption("controller", true, "controller URI");
        options.addOption("producers", true, "number of producers");
        options.addOption("consumers", true, "number of consumers");
        options.addOption("events", true, "number of events/records per producer/consumer");
        options.addOption("time", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message (event or record)");
        options.addOption("stream", true, "Stream name");
        options.addOption("randomkey", true, "Set Random key default is one key per producer");
        options.addOption("transactionspercommit", true, "Number of events before a transaction is committed");
        options.addOption("segments", true, "Number of segments");
        options.addOption("recreate", true, "If the stream is already existing, delete it and recreate it");

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
                eventsPerWorker = Integer.parseInt(commandline.getOptionValue("events"));
            }

            if (commandline.hasOption("time")) {
                runtimeSec = Integer.parseInt(commandline.getOptionValue("time"));
            }

            if (commandline.hasOption("transaction")) {
                isTransaction = Boolean.parseBoolean(commandline.getOptionValue("transaction"));
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

            if (commandline.hasOption("kafka")) {
                runKafka = Boolean.parseBoolean(commandline.getOptionValue("kafka"));
            }

            if (commandline.hasOption("segments")) {
                segmentCount = Integer.parseInt(commandline.getOptionValue("segments"));
            } else {
                segmentCount = producerCount;
            }

            if (commandline.hasOption("recreate")) {
                recreate = Boolean.parseBoolean(commandline.getOptionValue("recreate"));
            }
        }
    }

    private static class StartLocalService {
        static final int PORT = 9090;
        static final String SCOPE = "Scope";
        static final String STREAM_NAME = "aaj";
    }
}
