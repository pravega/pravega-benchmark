/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.perf;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.JavaSerializer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.Setter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;



/**
 * Performance benchmark for Pravega.
 * Data format is in comma separated format as following: {TimeStamp, Sensor Id, Location, TempValue }.
 *
 */
public class PravegaPerfTest {

    private static PerfStats produceStats, consumeStats, drainStats;
    private static String controllerUri = "tcp://localhost:9090";
    private static int messageSize = 100;
    private static String streamName = StartLocalService.STREAM_NAME;
    private static ClientFactory factory = null;
    private static boolean onlyWrite = true;
    private static boolean blocking = false;
    private static boolean fork = true;
    // How many producers should we run concurrently
    private static int producerCount = 20;
    private static int consumerCount = 20;
    private static int segmentCount = 20;
    // How many events each producer has to produce per seconds
    private static int eventsPerSec = 40;
    // How long it needs to run
    private static int runtimeSec = 10;
    // Should producers use Transaction or not
    private static boolean isTransaction = false;
    /*
     * recommended value for reporting interval ; 
     * its better to keep 1000ms(1 second) to align with eventspersec 
     */
    private static int reportingInterval = 1000;
    private static ScheduledExecutorService executor;
    private static ScheduledExecutorService bgexecutor;
    private static ForkJoinPool  fjexecutor;
    private static CountDownLatch latch;
    private static boolean runKafka = false;
    private static boolean isRandomKey = false;
    private static int transactionPerCommit = 1;

    public static void main(String[] args) throws Exception {

        final long StartTime = System.currentTimeMillis();

        parseCmdLine(args);

        // Initialize executor
        if (fork) {
           fjexecutor = new ForkJoinPool();
        } else {
           executor = Executors.newScheduledThreadPool(producerCount + consumerCount);
        } 
        bgexecutor = Executors.newScheduledThreadPool(10);
        try {
            @Cleanup StreamManager streamManager = null;
            StreamConfiguration streamconfig = null;
            streamManager = StreamManager.create(new URI(controllerUri));
            streamManager.createScope("Scope");
            streamconfig = StreamConfiguration.builder().scope("Scope").streamName(streamName)
                            .scalingPolicy(ScalingPolicy.fixed(segmentCount))
                            .build();

            if (!streamManager.createStream("Scope", streamName,streamconfig)) {
               System.out.println("The stream: " + streamName + " may already exists, so updating to "+ segmentCount+ " segments");
               if (!streamManager.updateStream("Scope", streamName,streamconfig)) {
                   System.out.println("Could not able to update the stream: "+streamName+ " try with another stream Name");
                   System.exit(1);
               } 
            }

            factory = ClientFactory.withScope("Scope", new URI(controllerUri));
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(1);
        }


        if ( !onlyWrite ) {
            ReaderGroupManager readerGroupManager = null;
            try {
                readerGroupManager = ReaderGroupManager.withScope("Scope", new URI(controllerUri));
            } catch (URISyntaxException e1) {
                e1.printStackTrace();
            }
            readerGroupManager.createReaderGroup(streamName,
                    ReaderGroupConfig.builder().build());
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup(streamName);
            consumeStats = new PerfStats("Reading", consumerCount * eventsPerSec * runtimeSec, reportingInterval,messageSize);
            drainStats = new PerfStats("Draining", consumerCount * eventsPerSec * runtimeSec, reportingInterval,
                    messageSize);
            ReaderWorker.setTotalEvents(new AtomicInteger(consumerCount * eventsPerSec * runtimeSec));
            for(int i=0;i<consumerCount;i++) {
                ReaderWorker reader = new ReaderWorker(i);
                if(i == 0)
                    reader.cleanupEvents();
                execute(reader);
            }
            if(consumerCount == 0)
               readerGroup.initiateCheckpoint(streamName, bgexecutor);
        }
        
        WriterWorker workers[] = new WriterWorker[producerCount];
        /* Create producerCount number of threads to simulate sensors. */
        latch = new CountDownLatch(producerCount);
        for (int i = 0; i < producerCount; i++) {
            //factory = new ClientFactoryImpl("Scope", new URI(controllerUri));

            if ( isTransaction ) {
                workers[i] = new TransactionWriterWorker(i, eventsPerSec,
                        runtimeSec,isTransaction, isRandomKey, 
                        transactionPerCommit, StartTime, factory);
            } else {
                workers[i] = new WriterWorker(i, eventsPerSec, runtimeSec,
                        isTransaction, isRandomKey, StartTime, factory);
            }
         } 

        produceStats = new PerfStats("Writing",producerCount * eventsPerSec * runtimeSec, reportingInterval,
                messageSize);          


         for (int i = 0; i < producerCount; i++) {
             execute(workers[i]);
        }

        latch.await();
        long endTime = System.currentTimeMillis(); 

        System.out.println("\nFinished all producers");
        if(producerCount != 0) {
            produceStats.printAll();
            produceStats.printTotal(endTime);
        }

        shutdown();
        // Wait until all threads are finished.
        awaitTermination(1, TimeUnit.HOURS);


        if ( !onlyWrite && consumerCount != 0 ) {
            consumeStats.printTotal(System.currentTimeMillis());
        }
        System.exit(0);
    }

   
    private static void execute(Runnable task ) throws Exception {
        if (fork) {
            fjexecutor.execute(task);
        } else  {
            executor.execute(task);
       }
    }


    private static void shutdown() throws Exception {
        if (fork) {
           fjexecutor.shutdown();
        } else  {
           executor.shutdown();
       }
    }
       
    private static boolean awaitTermination (long timeout,
                       TimeUnit unit) throws InterruptedException {
        if (fork) {
          return  fjexecutor.awaitTermination(timeout,unit);  
       } else {
          return  executor.awaitTermination(timeout,unit); 
       }    
    } 

    private static void parseCmdLine(String[] args) {
        // create Options object
        Options options = new Options();

        options.addOption("controller", true, "controller URI");
        options.addOption("producers", true, "number of producers");
        options.addOption("consumers", true, "number of consumers");
        options.addOption("eventspersec", true, "number events per sec");
        options.addOption("runtime", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message (record)");
        options.addOption("stream", true, "Stream name");
        options.addOption("writeonly", true, "Just produce vs read after produce");
        options.addOption("blocking", true, "Block for each ack");
        options.addOption("reporting", true, "Reporting internval in milliseconds, default set to 1000ms (1 sec)");
        options.addOption("randomkey", true, "Set Random key default is one key per producer");
        options.addOption("transactionspercommit", true, "Number of events before a transaction is committed");
        options.addOption("segments", true, "Number of segments");
        options.addOption("fork", true, "Use fork join framework for parallel threads");


        options.addOption("help", false, "Help message");

        CommandLineParser parser = new BasicParser();
        try {

            CommandLine commandline = parser.parse(options, args);
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

                if (commandline.hasOption("eventspersec")) {
                    eventsPerSec = Integer.parseInt(commandline.getOptionValue("eventspersec"));
                }

                if (commandline.hasOption("runtime")) {
                    runtimeSec = Integer.parseInt(commandline.getOptionValue("runtime"));
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

                if (commandline.hasOption("writeonly")) {
                    onlyWrite = Boolean.parseBoolean(commandline.getOptionValue("writeonly"));
                }
                if (commandline.hasOption("blocking")) {
                    blocking = Boolean.parseBoolean(commandline.getOptionValue("blocking"));
                }

                if (commandline.hasOption("reporting")) {
                    reportingInterval = Integer.parseInt(commandline.getOptionValue("reporting"));
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

                if (commandline.hasOption("fork")) {
                    fork = Boolean.parseBoolean(commandline.getOptionValue("fork"));
                }
            }
        } catch (Exception nfe) {
            System.out.println("Invalid arguments. Starting with default values");
            nfe.printStackTrace();
        }
    }

    /**
     * A Sensor simulator class that generates dummy value as temperature measurement and ingests to specified stream.
     */

    private static class WriterWorker implements Runnable {

        final EventStreamWriter<String> producer;
        private final int producerId;
        private final int eventsPerSec;
        private final int secondsToRun;
        private final boolean isTransaction;
	private final long StartTime;

        WriterWorker(int sensorId, int eventsPerSec, int secondsToRun, boolean isTransaction, boolean isRandomKey,
                     long start, ClientFactory factory) {
            this.producerId = sensorId;
            this.eventsPerSec = eventsPerSec;
            this.secondsToRun = secondsToRun;
            this.isTransaction = isTransaction;
            this.StartTime = start;
            this.producer = factory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());

        }

        /**
         * This function will be executed in a loop and time behavior is measured.
         * @return A function which takes String key and data and returns a future object.
         */
        BiFunction<String, String, CompletableFuture> sendFunction() {
            return  ( key, data) -> producer.writeEvent(key, data);
        }

        /**
         * Executes the given method over the producer with configured settings.
         * @param fn The function to execute.
         */
        void runLoop(BiFunction<String, String, CompletableFuture> fn) {

            CompletableFuture retFuture = null;
            final long Mseconds = secondsToRun*1000;
            long DiffTime = Mseconds;

            do {

                long loopStartTime = System.currentTimeMillis();
                for (int i = 0; i < eventsPerSec; i++)  {

                    // Construct event payload
                    String val = System.currentTimeMillis() + ", " + producerId + ", " + (int) (Math.random() * 200);
                    String payload = String.format("%-" + messageSize + "s", val);
                    String key;
                    if (isRandomKey) {
                        key = Integer.toString(producerId + new Random().nextInt());
                    } else {
                        key = Integer.toString(producerId);
                    }
                   
                    // event ingestion
                    long now = System.currentTimeMillis();
                    retFuture = produceStats.runAndRecordTime(() -> {
                                return fn.apply(key, payload);
                            },
                            now,
                            payload.length());
                    //If it is a blocking call, wait for the ack
                    if ( blocking ) {
                        try {
                            retFuture.get();
                        } catch (InterruptedException  | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }

                }

                long timeSpent = System.currentTimeMillis() - loopStartTime;
                // wait for next event
                try {
                     if (timeSpent < 1000) {
                          Thread.sleep(1000 - timeSpent);
                     }
                } catch (InterruptedException e) {
                    // log exception
                    System.exit(1);
                }

                DiffTime = System.currentTimeMillis() - StartTime; 
 
            } while(DiffTime < Mseconds);

            producer.flush();
            // producer.close();
            try {
                //Wait for the last packet to get acked
                retFuture.get();
            } catch (InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            runLoop(sendFunction());
            latch.countDown();
        }
    }


    private static class TransactionWriterWorker extends WriterWorker {

        private Transaction<String> transaction;
        private final int transactionsPerCommit;
        private int eventCount = 0;

        TransactionWriterWorker(int sensorId, int eventsPerSec, int secondsToRun, boolean
                isTransaction, boolean isRandomKey, int transactionsPerCommit, long start, ClientFactory factory) {
            super(sensorId, eventsPerSec, secondsToRun, isTransaction, isRandomKey, start, factory);
            this.transactionsPerCommit = transactionsPerCommit;
            transaction = producer.beginTxn();
        }

        BiFunction<String, String, CompletableFuture> sendFunction() {
            return  ( key, data) -> {
                try {
                    eventCount++;
                    transaction.writeEvent(key, data);
                    if (eventCount >= transactionsPerCommit) {
                        eventCount = 0;
                        transaction.commit();
                        transaction = producer.beginTxn();
                    }
                } catch (TxnFailedException e) {
                    System.out.println("Publish to transaction failed");
                    e.printStackTrace();
                }
                return null;

          };
        }
    }

    /**
     * A Sensor reader class that reads the temperative data
     */
    private static class ReaderWorker implements Runnable {
       @Setter
        public static AtomicInteger totalEvents;
        private EventStreamReader<String> reader;
        String readerId;


        public ReaderWorker(int readerId) {
            this.readerId = Integer.toString(readerId);
            ClientFactory clientFactory = ClientFactory.withScope("Scope",
                    URI.create(controllerUri));

            reader = clientFactory.createReader(
                    this.readerId, streamName, new JavaSerializer<String>(), ReaderConfig.builder().build());
        }

        public void cleanupEvents() {
            try {
                EventRead<String> result;
                System.out.format("******** Draining events from %s/%s%n", "Scope", streamName);
                do {
                    long startTime = System.currentTimeMillis();
                    result = reader.readNextEvent(600);
                    if(result.getEvent()!=null) {
                        drainStats.runAndRecordTime(() -> {
                            return null;
                        }, startTime, result.getEvent().length());
                    } else break;
                }while (true);
                drainStats.printTotal(System.currentTimeMillis());
            } catch (ReinitializationRequiredException e) {
                e.printStackTrace();
            }

        }
        @Override
        public void run() {
                System.out.format("******** Reading events from %s/%s%n", "Scope", streamName);
                EventRead<String> event = null;
                try {
                    int counter = 0;
                    do {
                        final EventRead<String> result = reader.readNextEvent(60000);
                        if(result.getEvent() == null)
                        {
                            continue;
                        }
                        counter = totalEvents.decrementAndGet();
                         consumeStats.runAndRecordTime(() -> {
                            return null;
                        }, Long.parseLong(result.getEvent().split(",")[0]), result.getEvent().length());

                    }while (counter > 0);
                } catch (ReinitializationRequiredException e) {
                    e.printStackTrace();
                }
                reader.close();
        }
    }


    private static class StartLocalService {
        static final int PORT = 9090;
        static final String SCOPE = "Scope";
        static final String STREAM_NAME = "aaj";
    }
}
