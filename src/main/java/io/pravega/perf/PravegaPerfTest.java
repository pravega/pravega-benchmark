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
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.ClientFactoryImpl;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;
import java.util.Locale;

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

        opt = new Option("controller", true, "controller URI");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("stream", true, "Stream name");
        opt.setRequired(true);
        options.addOption(opt);

        options.addOption("producers", true, "number of producers");
        options.addOption("consumers", true, "number of consumers");
        options.addOption("events", true,
                "number of events/records if 'time' not specified;\n" +
                        "otherwise, maximum events per second by producer(s) " +
                        "and/or number of events per consumer");
        options.addOption("time", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("transactionspercommit", true,
                "Number of events before a transaction is committed");
        options.addOption("segments", true, "Number of segments");
        options.addOption("size", true, "Size of each message (event or record)");
        options.addOption("recreate", true,
                "If the stream is already existing, delete it and recreate it (not applicable for Kafka)");
        options.addOption("throughput", true,
                "if > 0 , throughput in MB/s\n" +
                        "if 0 , writes 'events'\n" +
                        "if -1, get the maximum throughput");
        options.addOption("writecsv", true, "csv file to record write latencies");
        options.addOption("readcsv", true, "csv file to record read latencies");
        options.addOption("kafka", true, "Kafka Benchmarking");

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

            final List<Callable<Void>> workers = Stream.of(producers, consumers)
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.exit(0);
    }

    public static Test createTest(long startTime, CommandLine commandline, Options options) {
        try {
            boolean runKafka;
            if (commandline.hasOption("kafka")) {
                runKafka = Boolean.parseBoolean(commandline.getOptionValue("kafka"));
            } else {
                runKafka = false;
            }
            if (runKafka) {
                return new KafkaTest(startTime, commandline);
            } else {
                return new PravegaTest(startTime, commandline);
            }
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
        final int transactionPerCommit;
        final int runtimeSec;
        final double throughput;
        final String writeFile;
        final String readFile;
        final PerfStats produceStats;
        final PerfStats consumeStats;
        final long startTime;

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

            scopeName = SCOPE;
            if (producerCount == 0 && consumerCount == 0) {
                throw new IllegalArgumentException("Error: Must specify the number of producers or Consumers");
            }

            if (producerCount > 0) {
                if (messageSize == 0) {
                    throw new IllegalArgumentException("Error: Must specify the event 'size'");
                }

                writeAndRead = consumerCount > 0;

                if (writeAndRead) {
                    produceStats = null;
                } else {
                    produceStats = new PerfStats("Writing", REPORTINGINTERVAL, messageSize, writeFile);
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
                consumeStats = new PerfStats(action, REPORTINGINTERVAL, messageSize, readFile);
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

        public abstract List<WriterWorker> getProducers();

        public abstract List<ReaderWorker> getConsumers() throws URISyntaxException;

    }

    static private class PravegaTest extends Test {
        final PravegaStreamHandler streamHandle;
        final ClientFactory factory;
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

            streamHandle = new PravegaStreamHandler(scopeName, streamName, controllerUri,
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
                readerGroup = streamHandle.createReaderGroup();
            } else {
                readerGroup = null;
            }

            factory = new ClientFactoryImpl(scopeName, controller);
        }

        public List<WriterWorker> getProducers() {
            final List<WriterWorker> writers;

            if (producerCount > 0) {
                if (transactionPerCommit > 0) {
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new PravegaTransactionWriterWorker(i, eventsPerProducer,
                                    runtimeSec, false,
                                    messageSize, startTime,
                                    produceStats, streamName,
                                    eventsPerSec, writeAndRead, factory,
                                    transactionPerCommit))
                            .collect(Collectors.toList());
                } else {
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new PravegaWriterWorker(i, eventsPerProducer,
                                    runtimeSec, false,
                                    messageSize, startTime,
                                    produceStats, streamName,
                                    eventsPerSec, writeAndRead, factory))
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
                                streamName, TIMEOUT, writeAndRead, factory))
                        .collect(Collectors.toList());
            } else {
                readers = null;
            }
            return readers;
        }

        @Override
        public void shutdown(long endTime) {
            if (readerGroup != null) {
                readerGroup.close();
            }
            super.shutdown(endTime);
        }
    }

    static private class KafkaTest extends Test {
        final private Properties producerConfig;
        final private Properties consumerConfig;

        KafkaTest(long startTime, CommandLine commandline) throws
                IllegalArgumentException, URISyntaxException, InterruptedException, Exception {
            super(startTime, commandline);
            producerConfig = createProducerConfig();
            consumerConfig = createConsumerConfig();
        }

        private Properties createProducerConfig() {
            if (producerCount < 1) {
                return null;
            }
            final Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, controllerUri);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            // Enabling the producer IDEMPOTENCE is must to compare between Kafka and Pravega
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            return props;
        }

        private Properties createConsumerConfig() {
            if (consumerCount < 1) {
                return null;
            }
            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, controllerUri);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            // Enabling the consumer to READ_COMMITTED is must to compare between Kafka and Pravega
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
            if (writeAndRead) {
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                props.put(ConsumerConfig.GROUP_ID_CONFIG, streamName);
            } else {
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                props.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toString(startTime));
            }
            return props;
        }


        public List<WriterWorker> getProducers() {
            final List<WriterWorker> writers;

            if (producerCount > 0) {
                if (transactionPerCommit > 0) {
                    throw new IllegalArgumentException("Kafka Transactions are not supported");
                } else {
                    writers = IntStream.range(0, producerCount)
                            .boxed()
                            .map(i -> new KafkaWriterWorker(i, eventsPerProducer,
                                    runtimeSec, false,
                                    messageSize, startTime,
                                    produceStats, streamName,
                                    eventsPerSec, writeAndRead, producerConfig))
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
                        .map(i -> new KafkaReaderWorker(i, eventsPerConsumer,
                                runtimeSec, startTime, consumeStats,
                                streamName, TIMEOUT, writeAndRead, consumerConfig))
                        .collect(Collectors.toList());

            } else {
                readers = null;
            }
            return readers;
        }
    }

}
