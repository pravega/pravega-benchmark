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

    private static String controllerUri = "tcp://localhost:9090";
    private static int messageSize = 100;
    private static String streamName = StartLocalService.STREAM_NAME;
    private static String scopeName = StartLocalService.SCOPE;
    private static boolean recreate = false;
    private static int producerCount = 0;
    private static int consumerCount = 0;
    private static int segmentCount = 0;
    private static int eventsPerSec = 40;
    private static int runtimeSec = 10;
    private static boolean isTransaction = false;
    private static int reportingInterval = 1000;
    private static boolean runKafka = false;
    private static boolean isRandomKey = false;
    private static int transactionPerCommit = 1;


    public static void main(String[] args) throws Exception {

        final long StartTime = System.currentTimeMillis();
        long endTime; 
        ReaderGroup readerGroup=null;
        final int timeout=10;
        final ClientFactory factory;
        ControllerImpl controller = null;
        ScheduledExecutorService bgexecutor;
        ForkJoinPool  fjexecutor;
        final PerfStats produceStats, consumeStats, drainStats;
        final List <Callable<Void>> readers;
        final List <Callable<Void>> writers;



        parseCmdLine(args);

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
               
            StreamHandler  streamHandle =  new StreamHandler(scopeName, streamName, controllerUri,
                                                             segmentCount, timeout, controller,
                                                             bgexecutor);

            if (producerCount > 0 &&  !streamHandle.create()) {
               if (recreate)
                     streamHandle.recreate();
               else
                     streamHandle.scale();                    
            }

            factory = new ClientFactoryImpl(scopeName, controller);

            if (consumerCount > 0 ) {
               readerGroup = streamHandle.createReaderGroup();
               drainStats = new PerfStats("Draining", reportingInterval, messageSize);
               consumeStats = new PerfStats("Reading", reportingInterval, messageSize);
               ReaderWorker.totalEvents = new AtomicInteger(consumerCount * eventsPerSec * runtimeSec);


               readers = IntStream.range(0, consumerCount)
                                  .boxed()
                                  .map(i ->  new ReaderWorker(i, runtimeSec,
                                                             StartTime, factory,
                                                             consumeStats, streamName, timeout))
                                  .collect(Collectors.toList());

               if (producerCount > 0) {
                   ReaderWorker  r =  (ReaderWorker) readers.get(0);
                   r.cleanupEvents(drainStats);
               }
            } else {
              readers = null;
              drainStats =null;
              consumeStats = null;
            }


            if ( producerCount > 0) {

               produceStats = new PerfStats("Writing",reportingInterval, messageSize);
               if (isTransaction) { 

                   writers = IntStream.range(0, producerCount)
                              .boxed()
                              .map(i -> new TransactionWriterWorker(i, eventsPerSec,
                                             runtimeSec, isRandomKey,
                                             messageSize, StartTime,
                                             factory, produceStats,
                                             streamName, transactionPerCommit))
                              .collect(Collectors.toList());

               } else {

                   writers = IntStream.range(0, producerCount)
                              .boxed()
                              .map(i -> new WriterWorker(i, eventsPerSec,
                                             runtimeSec, isRandomKey,
                                             messageSize, StartTime,
                                             factory, produceStats,
                                             streamName))
                              .collect(Collectors.toList());
              }       

           } else {
             writers = null;
             produceStats = null;
           } 


           final List<Callable<Void>> workers = new ArrayList() {{
                                              if (readers != null)
			                          addAll(readers);
                                               if (writers != null)
			                          addAll(writers); }};
           fjexecutor.invokeAll(workers); 
           endTime = System.currentTimeMillis();  
           if(produceStats != null) {
              produceStats.printAll();
              produceStats.printTotal(endTime);
           }

           if ( consumeStats != null ) {
              consumeStats.printTotal(endTime);

           }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           fjexecutor.shutdown();
           fjexecutor.awaitTermination(runtimeSec, TimeUnit.SECONDS);
        }
            
        System.exit(0);
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
        options.addOption("reporting", true, "Reporting internval in milliseconds, default set to 1000ms (1 sec)");
        options.addOption("randomkey", true, "Set Random key default is one key per producer");
        options.addOption("transactionspercommit", true, "Number of events before a transaction is committed");
        options.addOption("segments", true, "Number of segments");
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

                if (commandline.hasOption("recreate")) {
                    recreate = Boolean.parseBoolean(commandline.getOptionValue("recreate"));
                }

            }
        } catch (Exception nfe) {
            System.out.println("Invalid arguments. Starting with default values");
            nfe.printStackTrace();
        }
    }


    private static class StartLocalService {
        static final int PORT = 9090;
        static final String SCOPE = "Scope";
        static final String STREAM_NAME = "aaj";
    }
}

class StreamHandler{
      final String scope;
      final String stream;
      final String controllerUri;   
      ControllerImpl controller; 
      StreamManager streamManager;
      StreamConfiguration streamconfig;
      ReaderGroupManager readerGroupManager; 
      ReaderGroup readerGroup;
      ScheduledExecutorService bgexecutor;
      final int segCount;
      final int timeout;


      StreamHandler(String scope, String stream, 
                    String uri, int segs,
                    int timeout,  ControllerImpl contrl,
                    ScheduledExecutorService bgexecutor) throws Exception {
               this.scope = scope;
               this.stream = stream;
               this.controllerUri= uri;
               this.controller = contrl;
               this.segCount = segs; 
               this.timeout = timeout;
               this.readerGroup = null;
               this.bgexecutor = bgexecutor;
               streamManager = StreamManager.create(new URI(uri));
               streamManager.createScope(scope);
               streamconfig = null;

       } 


       boolean create() throws Exception {
               if (streamconfig == null)
                   streamconfig = StreamConfiguration.builder().scope(scope).streamName(stream)
                                                 .scalingPolicy(ScalingPolicy.fixed(segCount))
                                                 .build();
   
               return streamManager.createStream(scope, stream,streamconfig);
       }  

       void scale() throws Exception {
               StreamSegments segments = controller.getCurrentSegments(scope, stream).join();
               final int nseg = segments.getSegments().size();
               System.out.println("Current segments of the stream: "+stream+ " = " + nseg);
                
               if (nseg == segCount)  return;  
                  
               System.out.println("The stream: " + stream + " will be manually scaling to "+ segCount+ " segments");

               /*
                * Note that the Upgrade stream API does not change the number of segments;
                * but it indicates with new number of segments.
                * after calling update stream , manual scaling is required
                */
               if (!streamManager.updateStream(scope, stream, streamconfig)) {
                    throw new Exception("Could not able to update the stream: " + stream + " try with another stream Name");
               }

               final double keyRangeChunk = 1.0 / segCount;
               final Map<Double, Double> keyRanges = IntStream.range(0, segCount)
                                                              .boxed()
                                                              .collect(
                                                               Collectors
                                                              .toMap(x -> x * keyRangeChunk,
                                                                      x->(x + 1) * keyRangeChunk));
               final List<Long> segmentList = segments.getSegments()
                                                      .stream()
                                                      .map(Segment::getSegmentId)
                                                      .collect(Collectors.toList());

               CompletableFuture <Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope,stream),
                                                                    segmentList,
                                                                    keyRanges,
                                                                    bgexecutor).getFuture();


               if (!scaleStatus.get(timeout, TimeUnit.SECONDS)){
                    throw new Exception("ERROR : Scale operation on stream "+ stream+" did not complete");
               }

               System.out.println("Number of Segments after manual scale: "+
                                   controller.getCurrentSegments(scope, stream)
                                             .get().getSegments().size());

       } 

       void recreate() throws Exception {
               System.out.println("Sealing and Deleteing the stream : " + stream + " and then recreating the same");
               CompletableFuture<Boolean> sealStatus =  controller.sealStream(scope, stream);
               if (!sealStatus.get(timeout, TimeUnit.SECONDS)) {
                    throw new Exception("ERROR : Segment sealing operation on stream "+ stream + " did not complete");
               }

               CompletableFuture<Boolean> status =  controller.deleteStream(scope, stream);
               if (!status.get(timeout, TimeUnit.SECONDS)) {
                    throw new Exception("ERROR : stream: "+ stream + " delete failed");
               }

               if (!streamManager.createStream(scope, stream, streamconfig)) {
                    throw new Exception("ERROR : stream: "+ stream +" recreation failed");
               }
       }

       ReaderGroup  createReaderGroup() throws Exception {
              if (readerGroup != null) return readerGroup;

              readerGroupManager = ReaderGroupManager.withScope(scope,
                                   ClientConfig.builder()
                                               .controllerURI(new URI(controllerUri)).build());
              readerGroupManager.createReaderGroup(stream,
                                 ReaderGroupConfig.builder()
                                                  .stream(Stream.of(scope, stream))
                                                  .build());
              readerGroup = readerGroupManager.getReaderGroup(stream);
              return readerGroup;
      }     

}


class WriterWorker implements Callable<Void> {
      final EventStreamWriter<String> producer;
      private final int producerId;
      private final int eventsPerSec;
      private final int secondsToRun;
      private final int messageSize; 
      private final long StartTime;
      private final boolean isRandomKey;
      private final PerfStats stats;

      WriterWorker(int sensorId, int eventsPerSec, int secondsToRun,
                   boolean isRandomKey, int messageSize,  long start,
                   ClientFactory factory, PerfStats stats,  String streamName) {
            this.producerId = sensorId;
            this.eventsPerSec = eventsPerSec;
            this.secondsToRun = secondsToRun;
            this.StartTime = start;
            this.stats = stats;
            this.isRandomKey = isRandomKey; 
            this.messageSize = messageSize;      
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
      void runLoop(BiFunction<String, String, CompletableFuture> fn) throws Exception {

            CompletableFuture retFuture = null;
            final long mSeconds = secondsToRun*1000;
            long diffTime = mSeconds;

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
                    retFuture = stats.writeAndRecordTime(() -> {
                                return fn.apply(key, payload);
                            },
                            payload.length());

                }

                long timeSpent = System.currentTimeMillis() - loopStartTime;
                     if (timeSpent < 1000) {
                          Thread.sleep(1000 - timeSpent);
                     }

                diffTime = System.currentTimeMillis() - StartTime; 
 
            } while(diffTime < mSeconds);

            producer.flush();
            // producer.close();
 
            //Wait for the last packet to get acked
            retFuture.get();
        }

      @Override
      public Void call() throws Exception  {
            runLoop(sendFunction());
            return null;   
      }
}


class TransactionWriterWorker extends WriterWorker {
      private Transaction<String> transaction;
      private final int transactionsPerCommit;
      private int eventCount = 0;

      TransactionWriterWorker(int sensorId, int eventsPerSec,
                              int secondsToRun, boolean isRandomKey,
                              int messageSize, long start,
                              ClientFactory factory, PerfStats stats,
                              String streamName, int transactionsPerCommit) {

            super(sensorId, eventsPerSec, secondsToRun, isRandomKey,
                  messageSize, start, factory, stats, streamName);

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

class ReaderWorker implements Callable<Void> {
       public static AtomicInteger totalEvents;
       private final EventStreamReader<String> reader;
       private final int secondsToRun;
       private final long StartTime;
       private final int timeout;
       private final PerfStats stats;
       String readerId;


       ReaderWorker (int readerId, int secondsToRun, long start,
                    ClientFactory factory, PerfStats stats,
                    String readergrp, int timeout) {
            this.readerId = Integer.toString(readerId);
            this.secondsToRun = secondsToRun; 
            this.StartTime=start;
            this.stats = stats; 
            this.timeout = timeout;

            reader = factory.createReader(
                    this.readerId, readergrp, new JavaSerializer<String>(), ReaderConfig.builder().build());
       }

       void cleanupEvents(PerfStats drainStats) throws Exception {
            EventRead<String> event;
            String ret = null; 
            do {
                long startTime = System.currentTimeMillis();
                event = reader.readNextEvent(timeout);
                ret = event.getEvent();
                if(ret !=null) {
                    drainStats.runAndRecordTime(() -> {
                       return null;
                    }, startTime, ret.length());
                }
            }while (ret != null);
            drainStats.printTotal(System.currentTimeMillis());
        }


        @Override
        public Void call() throws Exception {
             EventRead<String> event = null;
             String ret = null;
             final long mSeconds = secondsToRun*1000;
             long diffTime = mSeconds;
             int counter = 0;
             try {
                 do {
                     event = reader.readNextEvent(timeout);
                     ret = event.getEvent(); 
                     if(ret != null) {
                           stats.runAndRecordTime(() -> {
                                        return null;
                                        }, Long.parseLong(ret.split(",")[0]), ret.length());
                     }      
                     counter = totalEvents.decrementAndGet();
                     diffTime = System.currentTimeMillis() - StartTime;   
                 }while ((counter > 0) && (diffTime < mSeconds));
             } finally {
             reader.close();  
             }
             return null;
        }

}

