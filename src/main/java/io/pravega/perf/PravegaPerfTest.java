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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Performance benchmark for Pravega.
 * Data format is in comma separated format as following: {TimeStamp, Sensor Id, Location, TempValue }.
 */
public class PravegaPerfTest {
    final static String BENCHMARKNAME = "pravega-benchmark";

    public static void main(String[] args) {
        final Options options = new Options();
        final HelpFormatter formatter = new HelpFormatter();
        final CommandLineParser parser;
        CommandLine commandline = null;
        Option opt = null;
        final long startTime = System.currentTimeMillis();

        options.addOption("controller", true, "Controller URI");
        options.addOption("scope", true, "Scope name");
        options.addOption("stream", true, "Stream name");
        options.addOption("producers", true, "Number of producers");
        options.addOption("consumers", true, "Number of consumers");
        options.addOption("events", true,
                "Number of events/records if 'time' not specified;\n" +
                        "otherwise, Maximum events per second by producer(s) " +
                        "and/or Number of events per consumer");
        options.addOption("flush", true,
                "Each producer calls flush after writing <arg> number of of events/records; " +
                        "Not applicable, if both producers and consumers are specified");
        options.addOption("time", true, "Number of seconds the code runs");
        options.addOption("transactionspercommit", true,
                "Number of events before a transaction is committed");
        options.addOption("segments", true, "Number of segments");
        options.addOption("size", true, "Size of each message (event or record)");
        options.addOption("recreate", true,
                "If the stream is already existing, delete and recreate the same");
        options.addOption("throughput", true,
                "if > 0 , throughput in MB/s\n" +
                        "if 0 , writes 'events'\n" +
                        "if -1, get the maximum throughput");
        options.addOption("writecsv", true, "CSV file to record write latencies");
        options.addOption("readcsv", true, "CSV file to record read latencies");
        options.addOption("writethroughputcsv", true, "CSV file to record write throughput");
        options.addOption("readthroughputcsv", true, "CSV file to record read throughput");
        options.addOption("enableConnectionPooling", true, "Set to false to disable connection pooling");
        options.addOption("writeWatermarkPeriodMillis", true,
                "If -1 (default), watermarks will not be written.\n" +
                "If 0 and not using transactions, watermarks will be written after every event.\n" +
                "If >0 and not using transactions, watermarks will be written with a period of this many milliseconds.\n" +
                "If >= 0 and using transactions, watermarks will be written on each commit.");
        options.addOption("readWatermarkPeriodMillis", true,
                "If -1 (default), watermarks will not be read.\n" +
                "If >0, watermarks will be read with a period of this many milliseconds.");

        options.addOption("help", false, "Help message");

        parser = new DefaultParser();
        try {
            commandline = parser.parse(options, args);
        } catch (ParseException ex) {
            ex.printStackTrace();
            formatter.printHelp(BENCHMARKNAME, options);
            System.exit(0);
        }

        if (commandline.hasOption("help")) {
            formatter.printHelp(BENCHMARKNAME, options);
            System.exit(0);
        }

        final Test perfTest = createTest(startTime, commandline, options);
        if (perfTest == null) {
            System.exit(0);
        }

        final ForkJoinPool executor = new ForkJoinPool();

        try {
            final List<WriterWorker> producers = perfTest.getProducers();
            final List<ReaderWorker> consumers = perfTest.getConsumers();

            final List<Callable<Void>> workers = Stream.of(consumers, producers)
                    .filter(x -> x != null)
                    .flatMap(x -> x.stream())
                    .collect(Collectors.toList());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        System.out.println();
                        executor.shutdown();
                        executor.awaitTermination(1, TimeUnit.SECONDS);
                        perfTest.shutdown(System.currentTimeMillis());
                        if (consumers != null) {
                            consumers.forEach(ReaderWorker::close);
                        }
                        if (producers != null) {
                            producers.forEach(WriterWorker::close);
                        }
                        perfTest.closeReaderGroup();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            });
            perfTest.start(System.currentTimeMillis());
            executor.invokeAll(workers);
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            perfTest.shutdown(System.currentTimeMillis());
            if (consumers != null) {
                consumers.forEach(ReaderWorker::close);
            }
            if (producers != null) {
                producers.forEach(WriterWorker::close);
            }
            perfTest.closeReaderGroup();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.exit(0);
    }

    public static Test createTest(long startTime, CommandLine commandline, Options options) {
        try {
            return new PravegaTest(startTime, commandline);
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(BENCHMARKNAME, options);
        } catch (URISyntaxException | InterruptedException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    static private abstract class Test {
        static final int MAXTIME = 60 * 60 * 24;
        static final int REPORTINGINTERVAL = 5000;
        static final int TIMEOUT = 1000;
        static final String SCOPE = "Scope";

        final String controllerUri;
        final int messageSize;
        final String streamName;
        final String rdGrpName;
        final String scopeName;
        final boolean recreate;
        final boolean writeAndRead;
        final int producerCount;
        final int consumerCount;
        final int segmentCount;
        final int events;
        final int eventsPerSec;
        final int eventsPerProducer;
        final int eventsPerConsumer;
        final int EventsPerFlush;
        final int transactionPerCommit;
        final int runtimeSec;
        final double throughput;
        final String writeFile;
        final String readFile;
        final String writeThroughputFile;
        final String readThroughputFile;
        final PerfStats produceStats;
        final PerfStats consumeStats;
        final long startTime;
        final boolean enableConnectionPooling;
        final long writeWatermarkPeriodMillis;
        final long readWatermarkPeriodMillis;

        Test(long startTime, CommandLine commandline) throws IllegalArgumentException {
            this.startTime = startTime;
            if (commandline.hasOption("controller")) {
                controllerUri = commandline.getOptionValue("controller");
            } else {
                controllerUri = null;
            }

            if (commandline.hasOption("producers")) {
                producerCount = Integer.parseInt(commandline.getOptionValue("producers"));
            } else {
                producerCount = 0;
            }

            if (commandline.hasOption("consumers")) {
                consumerCount = Integer.parseInt(commandline.getOptionValue("consumers"));
            } else {
                consumerCount = 0;
            }

            if (commandline.hasOption("events")) {
                events = Integer.parseInt(commandline.getOptionValue("events"));
            } else {
                events = 0;
            }

            if (commandline.hasOption("flush")) {
                int flushEvents = Integer.parseInt(commandline.getOptionValue("flush"));
                if (flushEvents > 0) {
                    EventsPerFlush = flushEvents;
                } else {
                    EventsPerFlush = Integer.MAX_VALUE;
                }
            } else {
                EventsPerFlush = Integer.MAX_VALUE;
            }

            if (commandline.hasOption("time")) {
                runtimeSec = Integer.parseInt(commandline.getOptionValue("time"));
            } else if (events > 0) {
                runtimeSec = 0;
            } else {
                runtimeSec = MAXTIME;
            }

            if (commandline.hasOption("size")) {
                messageSize = Integer.parseInt(commandline.getOptionValue("size"));
            } else {
                messageSize = 0;
            }

            if (commandline.hasOption("stream")) {
                streamName = commandline.getOptionValue("stream");
            } else {
                streamName = null;
            }

            if (commandline.hasOption("scope")) {
                scopeName = commandline.getOptionValue("scope");
            } else {
                scopeName = SCOPE;
            }

            if (commandline.hasOption("transactionspercommit")) {
                transactionPerCommit = Integer.parseInt(commandline.getOptionValue("transactionspercommit"));
            } else {
                transactionPerCommit = 0;
            }

            if (commandline.hasOption("segments")) {
                segmentCount = Integer.parseInt(commandline.getOptionValue("segments"));
            } else {
                segmentCount = producerCount;
            }

            if (commandline.hasOption("recreate")) {
                recreate = Boolean.parseBoolean(commandline.getOptionValue("recreate"));
            } else {
                recreate = producerCount > 0 && consumerCount > 0;
            }

            if (commandline.hasOption("throughput")) {
                throughput = Double.parseDouble(commandline.getOptionValue("throughput"));
            } else {
                throughput = -1;
            }

            if (commandline.hasOption("writecsv")) {
                writeFile = commandline.getOptionValue("writecsv");
            } else {
                writeFile = null;
            }
            if (commandline.hasOption("readcsv")) {
                readFile = commandline.getOptionValue("readcsv");
            } else {
                readFile = null;
            }

            if (commandline.hasOption("writethroughputcsv")) {
                writeThroughputFile = commandline.getOptionValue("writethroughputcsv");
            } else {
                writeThroughputFile = null;
            }
            if (commandline.hasOption("readthroughputcsv")) {
                readThroughputFile = commandline.getOptionValue("readthroughputcsv");
            } else {
                readThroughputFile = null;
            }

            enableConnectionPooling = Boolean.parseBoolean(commandline.getOptionValue("enableConnectionPooling", "true"));

            writeWatermarkPeriodMillis = Long.parseLong(commandline.getOptionValue("writeWatermarkPeriodMillis", "-1"));
            readWatermarkPeriodMillis = Long.parseLong(commandline.getOptionValue("readWatermarkPeriodMillis", "-1"));

            if (controllerUri == null) {
                throw new IllegalArgumentException("Error: Must specify Controller IP address");
            }

            if (streamName == null) {
                throw new IllegalArgumentException("Error: Must specify stream Name");
            }

            if (producerCount == 0 && consumerCount == 0) {
                throw new IllegalArgumentException("Error: Must specify the number of producers or Consumers");
            }

            if (recreate) {
                rdGrpName = streamName + startTime;
            } else {
                rdGrpName = streamName + "RdGrp";
            }

            if (producerCount > 0) {
                if (messageSize == 0) {
                    throw new IllegalArgumentException("Error: Must specify the event 'size'");
                }

                writeAndRead = consumerCount > 0;

                if (writeAndRead) {
                    produceStats = null;
                } else {
                    produceStats = new PerfStats("Writing", REPORTINGINTERVAL, messageSize, writeFile, writeThroughputFile);
                }

                eventsPerProducer = (events + producerCount - 1) / producerCount;
                if (throughput < 0 && runtimeSec > 0) {
                    eventsPerSec = events / producerCount;
                } else if (throughput > 0) {
                    eventsPerSec = (int) (((throughput * 1024 * 1024) / messageSize) / producerCount);
                } else {
                    eventsPerSec = 0;
                }
            } else {
                produceStats = null;
                eventsPerProducer = 0;
                eventsPerSec = 0;
                writeAndRead = false;
            }

            if (consumerCount > 0) {
                String action;
                if (writeAndRead) {
                    action = "Write/Reading";
                } else {
                    action = "Reading";
                }
                consumeStats = new PerfStats(action, REPORTINGINTERVAL, messageSize, readFile, readThroughputFile);
                eventsPerConsumer = events / consumerCount;
            } else {
                consumeStats = null;
                eventsPerConsumer = 0;
            }
        }

        public void start(long startTime) throws IOException {
            if (produceStats != null && !writeAndRead) {
                produceStats.start(startTime);
            }
            if (consumeStats != null) {
                consumeStats.start(startTime);
            }
        }

        public void shutdown(long endTime) {
            try {
                if (produceStats != null && !writeAndRead) {
                    produceStats.shutdown(endTime);
                }
                if (consumeStats != null) {
                    consumeStats.shutdown(endTime);
                }
            } catch (ExecutionException | InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        public abstract void closeReaderGroup();

        public abstract List<WriterWorker> getProducers();

        public abstract List<ReaderWorker> getConsumers() throws URISyntaxException;

    }

    static private class PravegaTest extends Test {
        final PravegaStreamHandler streamHandle;
        final EventStreamClientFactory factory;
        final ReaderGroup readerGroup;

        PravegaTest(long startTime, CommandLine commandline) throws
                IllegalArgumentException, URISyntaxException, InterruptedException, Exception {
            super(startTime, commandline);
            final ScheduledExecutorService bgExecutor = Executors.newScheduledThreadPool(10);
            final ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                    .clientConfig(ClientConfig.builder()
                            .controllerURI(new URI(controllerUri)).build())
                    .maxBackoffMillis(5000).build(),
                    bgExecutor);

            streamHandle = new PravegaStreamHandler(scopeName, streamName, rdGrpName, controllerUri,
                    segmentCount, TIMEOUT, controller,
                    bgExecutor);

            if (producerCount > 0 && !streamHandle.create()) {
                if (recreate) {
                    streamHandle.recreate();
                } else {
                    streamHandle.scale();
                }
            }
            if (consumerCount > 0) {
                readerGroup = streamHandle.createReaderGroup(!writeAndRead);
            } else {
                readerGroup = null;
            }

            factory = new ClientFactoryImpl(scopeName, controller);
        }

        public List<WriterWorker> getProducers() {
            final List<WriterWorker> writers;

            if (producerCount > 0) {
                if (transactionPerCommit > 0) {
                    final boolean enableWatermark = writeWatermarkPeriodMillis >= 0;
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new PravegaTransactionWriterWorker(i, eventsPerProducer,
                                    runtimeSec, false,
                                    messageSize, startTime,
                                    produceStats, streamName,
                                    eventsPerSec, writeAndRead, factory,
                                    transactionPerCommit, enableConnectionPooling,
                                    enableWatermark))
                            .collect(Collectors.toList());
                } else {
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new PravegaWriterWorker(i, eventsPerProducer,
                                    EventsPerFlush, runtimeSec, false,
                                    messageSize, startTime, produceStats,
                                    streamName, eventsPerSec, writeAndRead, factory, enableConnectionPooling,
                                    writeWatermarkPeriodMillis))
                            .collect(Collectors.toList());
                }
            } else {
                writers = null;
            }

            return writers;
        }

        public List<ReaderWorker> getConsumers() throws URISyntaxException {
            final List<ReaderWorker> readers;
            if (consumerCount > 0) {
                readers = IntStream.range(0, consumerCount)
                        .boxed()
                        .map(i -> new PravegaReaderWorker(i, eventsPerConsumer,
                                runtimeSec, startTime, consumeStats,
                                rdGrpName, TIMEOUT, writeAndRead, factory,
                                io.pravega.client.stream.Stream.of(scopeName, streamName),
                                readWatermarkPeriodMillis))
                        .collect(Collectors.toList());
            } else {
                readers = null;
            }
            return readers;
        }

        @Override
        public void closeReaderGroup() {
            if (readerGroup != null) {
                readerGroup.close();
            }
        }

    }
}
