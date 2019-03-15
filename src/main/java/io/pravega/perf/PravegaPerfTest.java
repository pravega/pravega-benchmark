/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.perf;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.ClientFactoryImpl;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.IntStream;

/**
 * Performance benchmark for Pravega.
 * Data format is in comma separated format as following: {TimeStamp, Sensor Id, Location, TempValue }.
 */
public class PravegaPerfTest {
    private static final int maxTime = 60 * 60 * 24;
    private static String controllerUri = "tcp://localhost:9090";
    private static int messageSize = 100;
    private static String streamName = StartLocalService.STREAM_NAME;
    private static String scopeName = StartLocalService.SCOPE;
    private static boolean recreate = false;
    private static boolean writeNread = false;
    private static int producerCount = 0;
    private static int consumerCount = 0;
    private static int segmentCount = 0;
    private static int events = 0;
    private static int eventsPerSec = 0;
    private static int eventsPerWorker = 0;
    private static int transactionPerCommit = 0;
    private static int runtimeSec = 0;
    private static final int reportingInterval = 5000;
    private static ScheduledExecutorService bgexecutor;
    private static ForkJoinPool fjexecutor;
    private static PerfStats produceStats = null;
    private static PerfStats consumeStats = null;
    private static double throughput = 0;
    private static String writeFile = null;
    private static String readFile = null;
    private static ReaderGroup readerGroup = null;
    private static final int timeout = 10;
    private static ClientFactory factory = null;
    private static ControllerImpl controller = null;
    private static List<Callable<Void>> readers = null;
    private static List<Callable<Void>> writers = null;
    private static long startTime;

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();

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

        if (producerCount > 0 && consumerCount > 0) {
            writeNread = true;
            recreate = true;
        }

        if (events == 0) {
            if (throughput == 0) {
                throughput = -1;
            }
            if (runtimeSec == 0) {
                runtimeSec = maxTime;
            }
        }

        bgexecutor = Executors.newScheduledThreadPool(10);
        fjexecutor = new ForkJoinPool();


        try {
            setupPravega();
            final List<Callable<Void>> workers = Stream.of(readers, writers)
                    .filter(x -> x != null)
                    .flatMap(x -> x.stream())
                    .collect(Collectors.toList());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println();
                    shutdown();
                }
            });

            final long beginTime = System.currentTimeMillis();
            if (consumeStats != null) {
                consumeStats.start(beginTime);
            }
            if (produceStats != null) {
                produceStats.start(beginTime);
            }
            fjexecutor.invokeAll(workers);
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutdown();
        System.exit(0);
    }

    private static void setupPravega() throws
            URISyntaxException, InterruptedException, ReinitializationRequiredException, ExecutionException, TimeoutException, Exception {
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

        if (producerCount > 0) {
            if (writeNread) {
                produceStats = null;
            } else {
                produceStats = new PerfStats("Writing", reportingInterval, messageSize, writeFile);
            }
            eventsPerWorker = (events + producerCount - 1) / producerCount;
            if (throughput == 0 && runtimeSec > 0) {
                eventsPerSec = events / producerCount;
            } else if (throughput > 0) {
                eventsPerSec = (int) (((throughput * 1024 * 1024) / messageSize) / producerCount);
            }

            if (transactionPerCommit > 0) {
                writers = IntStream.range(0, producerCount)
                        .boxed()
                        .map(i -> new PravegaTransactionWriterWorker(i, eventsPerWorker,
                                runtimeSec, false,
                                messageSize, startTime,
                                produceStats, streamName,
                                eventsPerSec, writeNread, factory,
                                transactionPerCommit))
                        .collect(Collectors.toList());
            } else {
                writers = IntStream.range(0, producerCount)
                        .boxed()
                        .map(i -> new PravegaWriterWorker(i, eventsPerWorker,
                                runtimeSec, false,
                                messageSize, startTime,
                                produceStats, streamName,
                                eventsPerSec, writeNread, factory))
                        .collect(Collectors.toList());
            }
        } else {
            writers = null;
            produceStats = null;
        }

        if (consumerCount > 0) {
            readerGroup = streamHandle.createReaderGroup();
            String action;
            if (writeNread) {
                action = "Write/Reading";
            } else {
                action = "Reading";
            }
            consumeStats = new PerfStats(action, reportingInterval, messageSize, readFile);
            eventsPerWorker = events / consumerCount;
            readers = IntStream.range(0, consumerCount)
                    .boxed()
                    .map(i -> new PravegaReaderWorker(i, eventsPerWorker,
                            runtimeSec, startTime, consumeStats,
                            streamName, timeout, writeNread, factory))
                    .collect(Collectors.toList());
        } else {
            readers = null;
            consumeStats = null;
        }
    }

    private static synchronized void shutdown() {
        final long endTime = System.currentTimeMillis();
        if (fjexecutor == null) {
            return;
        }
        fjexecutor.shutdown();
        try {
            fjexecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        fjexecutor = null;
        if (produceStats != null) {
            produceStats.shutdown(endTime);
        }
        if (consumeStats != null) {
            consumeStats.shutdown(endTime);
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
                "number of events/records if 'time' not specified;\n" +
                        "otherwise, maximum events per second by producer(s)" +
                        "and/or number of events per consumer");
        options.addOption("time", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message (event or record)");
        options.addOption("stream", true, "Stream name");
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
