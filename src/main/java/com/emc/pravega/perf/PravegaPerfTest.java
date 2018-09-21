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

import io.pravega.client.ClientConfig;
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
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.segment.impl.Segment;
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
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;


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
    private static String scopeName = StartLocalService.SCOPE;
    private static ClientFactory factory = null;
    private static boolean blocking = false;
    private static boolean recreate = false;
    private static boolean fork = true;
    // How many producers should we run concurrently
    private static int producerCount = 0;
    private static int consumerCount = 0;
    private static int segmentCount = 0;
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
    private static ControllerImpl controller = null;
    private static int timeout = 10;
    private static ReaderGroupManager readerGroupManager = null;


    public static void main(String[] args) throws Exception {

        final long StartTime = System.currentTimeMillis();
        List <ReaderWorker> readers = null;
        List <WriterWorker> workers = null;
        ReaderGroup readerGroup=null;
          

        parseCmdLine(args);

        if (producerCount == 0 && consumerCount == 0) {
           System.out.println("Error: Must specify the number of producers or Consumers");
           System.exit(1);
        }

        final String readergrp =streamName;

        // Initialize executor
        bgexecutor = Executors.newScheduledThreadPool(10);


        try {
            @Cleanup StreamManager streamManager = null;
            StreamConfiguration streamconfig = null;
            URI uri= new URI(controllerUri);
            streamManager = StreamManager.create(uri);
            streamManager.createScope(scopeName);
           
            controller = new ControllerImpl(ControllerImplConfig.builder()
                                    .clientConfig(ClientConfig.builder().controllerURI(uri).build())
                                    .maxBackoffMillis(5000).build(),
                                     bgexecutor);

            if (producerCount > 0 ) {
                streamconfig = StreamConfiguration.builder().scope(scopeName).streamName(streamName)
                                                  .scalingPolicy(ScalingPolicy.fixed(segmentCount))
                                                  .build();

                if (!streamManager.createStream(scopeName, streamName,streamconfig)) {

                    StreamSegments segments = controller.getCurrentSegments(scopeName, streamName).join();
         
                    final int nseg = segments.getSegments().size();
                    System.out.println("Current segments of the stream: "+streamName+ " = " + nseg);  

                   if (!recreate ) {
                      if (nseg != segmentCount) {
                          System.out.println("The stream: " + streamName + " will be manually scaling to "+ segmentCount+ " segments");

                         /*
                          * Note that the Upgrade stream API does not change the number of segments; 
                          * but it indicates with new number of segments.
                          * after calling update stream , manual scaling is required
                          */   
                          if (!streamManager.updateStream(scopeName, streamName,streamconfig)) {
                              System.out.println("Could not able to update the stream: "+streamName+ " try with another stream Name");
                              System.exit(1);
                          }

                          final double keyRangeChunk = 1.0 / segmentCount;
                          final Map<Double, Double> keyRanges = IntStream.range(0, segmentCount)
                                                                 .boxed()
                                                                 .collect(Collectors.toMap(x -> x * keyRangeChunk ,  x->(x + 1) * keyRangeChunk)); 
                          final List<Long> segmentList = segments.getSegments().stream().map(Segment::getSegmentId).collect(Collectors.toList());

                          /*      
                          System.out.println("The key ranges are");
                          Map<Double, Double> map = new TreeMap<Double, Double>(keyRanges);

                          map.forEach((k,v) -> System.out.println("( "+k+", "+v +")"));               
                          System.out.println("segments list:"+segmentList);
                          */
                          CompletableFuture <Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scopeName,streamName),
                                                                       segmentList,
                                                                       keyRanges,
                                                                       bgexecutor).getFuture();

              
                          if (!scaleStatus.get(timeout, TimeUnit.SECONDS)){
                              System.out.println("ERROR : Scale operation on stream "+ streamName+" did not complete");
                              System.exit(1);
                          }

                          System.out.println("Number of Segments after manual scale: "+controller.getCurrentSegments(scopeName, streamName)
                                                                  .get().getSegments().size());
                       }
         
                  } else {
                       System.out.println("Sealing and Deleteing the stream : "+streamName+" and then recreating the same");
                       CompletableFuture<Boolean> sealStatus =  controller.sealStream(scopeName, streamName);
                       if (!sealStatus.get(timeout, TimeUnit.SECONDS)) {
                           System.out.println("ERROR : Segment sealing operation on stream "+ streamName+" did not complete");
                           System.exit(1);
                       }

                       CompletableFuture<Boolean> status =  controller.deleteStream(scopeName, streamName);
                       if (!status.get(timeout, TimeUnit.SECONDS)) {
                          System.out.println("ERROR : stream: "+ streamName+" delete failed");
                          System.exit(1);
                       }

                       if (!streamManager.createStream(scopeName, streamName,streamconfig)) {
                          System.out.println("ERROR : stream: "+ streamName+" recreation failed");
                          System.exit(1);
                       }   

                  }
                } 
            }

            if ( consumerCount > 0 ) {
                readerGroupManager = ReaderGroupManager.withScope(scopeName, ClientConfig.builder().controllerURI(new URI(controllerUri)).build());
                readerGroupManager.createReaderGroup(readergrp,
                                   ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build());
                readerGroup = readerGroupManager.getReaderGroup(readergrp);
            }     


            factory = new ClientFactoryImpl(scopeName, controller);  

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        if (fork) {
           fjexecutor = new ForkJoinPool();
        } else {
           executor = Executors.newScheduledThreadPool(producerCount + consumerCount);
        }


        /* Create producerCount number of threads to simulate sensors. */
        latch = new CountDownLatch(producerCount+consumerCount);

        try { 

            if (consumerCount > 0 ) {
                drainStats = new PerfStats("Draining", reportingInterval, messageSize);
                ReaderWorker.setTotalEvents(new AtomicInteger(consumerCount * eventsPerSec * runtimeSec));

                /* 
                if(consumerCount == 0)
                   readerGroup.initiateCheckpoint(streamName, bgexecutor);
                */

                readers = IntStream.range(0, consumerCount)
                                   .boxed()
                                   .map(i -> new ReaderWorker(i, runtimeSec, readergrp,  StartTime, factory)) 
                                   .collect(Collectors.toList());

                if (producerCount > 0) {
                    ReaderWorker r = readers.get(0);
                    r.cleanupEvents();
                }

            }


            if ( producerCount > 0) {

               if (isTransaction) { 
                   workers = IntStream.range(0, producerCount)
                               .boxed()
                               .map(i -> new TransactionWriterWorker(i, eventsPerSec,
                                             runtimeSec,isTransaction, isRandomKey,
                                             transactionPerCommit, StartTime, factory))
                               .collect(Collectors.toList());
               } else {

                   workers = IntStream.range(0, producerCount)
                               .boxed()
                               .map(i -> new WriterWorker(i, eventsPerSec, runtimeSec,
                                             isTransaction, isRandomKey, StartTime, factory))
                               .collect(Collectors.toList());

              }       

              produceStats = new PerfStats("Writing",reportingInterval, messageSize);          
              workers.forEach(w-> execute(w));
           }
 
           if (consumerCount > 0 ) {
               consumeStats = new PerfStats("Reading", reportingInterval, messageSize);
               readers.forEach(r->execute(r));
           }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }


        latch.await();
        
        long endTime = System.currentTimeMillis(); 
      
        System.out.println("\nFinished all producers");
        if(producerCount != 0) {
            produceStats.printAll();
            produceStats.printTotal(endTime);
        }

        if ( consumerCount > 0 ) {
            consumeStats.printTotal(endTime);

        }

        shutdown();
        awaitTermination(runtimeSec, TimeUnit.SECONDS);

        System.exit(0);
    }

   
    private static void execute(Runnable task ) {
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
        options.addOption("recreate", true, "If the stream is already existing, delete it and recreate it");

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

                if (commandline.hasOption("recreate")) {
                    recreate = Boolean.parseBoolean(commandline.getOptionValue("recreate"));
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
                    retFuture = produceStats.writeAndRecordTime(() -> {
                                return fn.apply(key, payload);
                            },
                            payload.length(), blocking);

                }

                long timeSpent = System.currentTimeMillis() - loopStartTime;
                // wait for next event
                try {
                     if (timeSpent < 1000) {
                          Thread.sleep(1000 - timeSpent);
                     }
                } catch (InterruptedException e) {
                    // log exception
                    e.printStackTrace();
                    System.exit(1);
                }

                DiffTime = System.currentTimeMillis() - StartTime; 
 
            } while(DiffTime < Mseconds);

            producer.flush();
            // producer.close();
            
            if (!blocking) {
                try {
                   //Wait for the last packet to get acked
                   retFuture.get();
                } catch (InterruptedException | ExecutionException e ) {
                   e.printStackTrace();
                }
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
        private final int secondsToRun;
        private final long StartTime;
        String readerId;


        public ReaderWorker(int readerId, int secondsToRun, String readergrp,  long start,  ClientFactory factory) {
            this.readerId = Integer.toString(readerId);
            this.secondsToRun = secondsToRun; 
            this.StartTime=start;

            reader = factory.createReader(
                    this.readerId, readergrp, new JavaSerializer<String>(), ReaderConfig.builder().build());
        }

        public void cleanupEvents() {
            try {
                EventRead<String> result;
                System.out.format("******** Draining events from %s/%s%n", scopeName, streamName);
                do {
                    long startTime = System.currentTimeMillis();
                    result = reader.readNextEvent(timeout);
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

        public void close(){
             reader.close();
        }

        @Override
        public void run() {
                System.out.format("******** Reading events from %s/%s%n", scopeName , streamName);
                EventRead<String> event = null;
                final long Mseconds = secondsToRun*1000;
                long DiffTime = Mseconds;

                try {
                    int counter = 0;
                    do {
                        final EventRead<String> result = reader.readNextEvent(timeout);
                        
                        if(result.getEvent() != null)
                        {
                           consumeStats.runAndRecordTime(() -> {
                                        return null;
                                        }, Long.parseLong(result.getEvent().split(",")[0]), result.getEvent().length());
                        } 
                        counter = totalEvents.decrementAndGet();
                        DiffTime = System.currentTimeMillis() - StartTime;   
                        
                    }while ((counter > 0) && (DiffTime < Mseconds));
                } catch (ReinitializationRequiredException e) {
                    e.printStackTrace();
                }
                close();  
                latch.countDown();   
        }

    }


    private static class StartLocalService {
        static final int PORT = 9090;
        static final String SCOPE = "Scope";
        static final String STREAM_NAME = "aaj";
    }
}
