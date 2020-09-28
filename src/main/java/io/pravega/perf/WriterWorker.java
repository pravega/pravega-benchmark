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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract class for Writers.
 */
public abstract class WriterWorker extends Worker implements Callable<Void> {
    private static Logger log = LoggerFactory.getLogger(WriterWorker.class);

    final private static int MS_PER_SEC = 1000;
    final private int ROUTING_KEY_NUM = 10;
    final private Random random = new Random();
    final private String[] routingKeyArray;
    final private Performance perf;
    //    final private byte[] payload;
    final private int eventsPerSec;
    final private int EventsPerFlush;
    final private boolean writeAndRead;
    final private AtomicLong[] seqNum;
    final private Boolean isEnableRoutingKey;

    WriterWorker(int sensorId, int events, int EventsPerFlush, int secondsToRun,
                 boolean isRandomKey, int messageSize, long start,
                 PerfStats stats, String streamName, int eventsPerSec, boolean writeAndRead, AtomicLong[] seqNum,
                 Boolean isEnableRoutingKey) {

        super(sensorId, events, secondsToRun, messageSize, start, stats, streamName, 0);
        this.eventsPerSec = eventsPerSec;
        this.EventsPerFlush = EventsPerFlush;
        this.writeAndRead = writeAndRead;
//        this.payload = createPayload();
        this.perf = createBenchmark();
        this.seqNum = seqNum;
        this.isEnableRoutingKey = isEnableRoutingKey;
        this.routingKeyArray = getRoutingKeyArray();
    }

    private String[] getRoutingKeyArray() {
        String[] routingKeys = new String[ROUTING_KEY_NUM];
        for (int i = 0; i < ROUTING_KEY_NUM; i++) {
            routingKeys[i] = "routingKey" + i;
        }
        return routingKeys;
    }


    private byte[] createPayload() {
        String event;
        if(this.isEnableRoutingKey) {
            int index = random.nextInt(ROUTING_KEY_NUM);
            Long count = seqNum[index].getAndIncrement();
            String routingKey = routingKeyArray[index];
            event = streamName + "-" + routingKey + "-" + count + "-" + System.currentTimeMillis();
        } else {
            Long count = seqNum[0].getAndIncrement();
            event = streamName + "-" + count + "-" + System.currentTimeMillis();
        }

        return event.getBytes();
    }


    private Performance createBenchmark() {
        final Performance perfWriter;
        if (secondsToRun > 0) {
            if (writeAndRead) {
                perfWriter = this::EventsWriterTimeRW;
            } else {
                if (eventsPerSec > 0 || EventsPerFlush < Integer.MAX_VALUE) {
                    perfWriter = this::EventsWriterTimeSleep;
                } else {
                    perfWriter = this::EventsWriterTime;
                }
            }
        } else {
            if (writeAndRead) {
                perfWriter = this::EventsWriterRW;
            } else {
                if (eventsPerSec > 0 || EventsPerFlush < Integer.MAX_VALUE) {
                    perfWriter = this::EventsWriterSleep;
                } else {
                    perfWriter = this::EventsWriter;
                }
            }
        }
        return perfWriter;
    }


    /**
     * Writes the data and benchmark.
     *
     * @param data   data to write
     * @param record to call for benchmarking
     * @return time return the data sent time
     */
    public abstract long recordWrite(byte[] data, TriConsumer record);

    /**
     * Writes the data and benchmark.
     *
     * @param data data to write
     */
    public abstract void writeData(byte[] data);

    /**
     * Flush the producer data.
     */
    public abstract void flush();

    /**
     * Flush the producer data.
     */
    public abstract void close();


    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        try {
            perf.benchmark();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return null;
    }


    private void EventsWriter() throws InterruptedException, IOException {
        log.info("EventsWriter: Running");
        for (int i = 0; i < events; i++) {
            byte[] data = createPayload();
            recordWrite(data, stats::recordTime);
        }
        flush();
    }


    private void EventsWriterSleep() throws InterruptedException, IOException {
        log.info("EventsWriterSleep: Running");
        final EventsController eCnt = new EventsController(System.currentTimeMillis(), eventsPerSec);
        int cnt = 0;
        while (cnt < events) {
            int loopMax = Math.min(EventsPerFlush, events - cnt);
            for (int i = 0; i < loopMax; i++) {
                byte[] data = createPayload();
                eCnt.control(cnt++, recordWrite(data, stats::recordTime));
            }
            flush();
        }
    }


    private void EventsWriterTime() throws InterruptedException, IOException {
        log.info("EventsWriterTime: Running");
        final long msToRun = secondsToRun * MS_PER_SEC;
        long time = System.currentTimeMillis();
        while ((time - startTime) < msToRun) {
            byte[] data = createPayload();
            time = recordWrite(data, stats::recordTime);
        }
        flush();
    }


    private void EventsWriterTimeSleep() throws InterruptedException, IOException {
        log.info("EventsWriterTimeSleep: Running");
        final long msToRun = secondsToRun * MS_PER_SEC;
        long time = System.currentTimeMillis();
        final EventsController eCnt = new EventsController(time, eventsPerSec);
        long msElapsed = time - startTime;
        int cnt = 0;
        while (msElapsed < msToRun) {
            for (int i = 0; (msElapsed < msToRun) && (i < EventsPerFlush); i++) {
                byte[] data = createPayload();
                time = recordWrite(data, stats::recordTime);
                eCnt.control(cnt++, time);
                msElapsed = time - startTime;
            }
            flush();
        }
    }


    private void EventsWriterRW() throws InterruptedException, IOException {
        log.info("EventsWriterRW: Running");
        final ByteBuffer timeBuffer = ByteBuffer.allocate(TIME_HEADER_SIZE);
        final long time = System.currentTimeMillis();
        final EventsController eCnt = new EventsController(time, eventsPerSec);
        for (int i = 0; i < events; i++) {
            byte[] data = createPayload();
            byte[] bytes = timeBuffer.putLong(0, System.currentTimeMillis()).array();
            System.arraycopy(bytes, 0, data, 0, bytes.length);
            writeData(data);
                /*
                flush is required here for following reasons:
                1. The writeData is called for End to End latency mode; hence make sure that data is sent.
                2. In case of kafka benchmarking, the buffering makes too many writes;
                   flushing moderates the kafka producer.
                3. If the flush called after several iterations, then flush may take too much of time.
                */
            eCnt.control(i);
        }
        flush();
    }


    private void EventsWriterTimeRW() throws InterruptedException, IOException {
        log.info("EventsWriterTimeRW: Running");
        final long msToRun = secondsToRun * MS_PER_SEC;
        final ByteBuffer timeBuffer = ByteBuffer.allocate(TIME_HEADER_SIZE);
        long time = System.currentTimeMillis();
        final EventsController eCnt = new EventsController(time, eventsPerSec);

        for (int i = 0; (time - startTime) < msToRun; i++) {
            byte[] data = createPayload();
            time = System.currentTimeMillis();
            byte[] bytes = timeBuffer.putLong(0, System.currentTimeMillis()).array();
            System.arraycopy(bytes, 0, data, 0, bytes.length);
            writeData(data);
                /*
                flush is required here for following reasons:
                1. The writeData is called for End to End latency mode; hence make sure that data is sent.
                2. In case of kafka benchmarking, the buffering makes too many writes;
                   flushing moderates the kafka producer.
                3. If the flush called after several iterations, then flush may take too much of time.
                */
            eCnt.control(i);
        }
        flush();
    }


    @NotThreadSafe
    final static private class EventsController {
        private static final long NS_PER_MS = 1000000L;
        private static final long NS_PER_SEC = 1000 * NS_PER_MS;
        private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;
        private final long startTime;
        private final long sleepTimeNs;
        private final int eventsPerSec;
        private long toSleepNs = 0;

        /**
         * @param eventsPerSec events per second
         */
        private EventsController(long start, int eventsPerSec) {
            this.startTime = start;
            this.eventsPerSec = eventsPerSec;
            this.sleepTimeNs = this.eventsPerSec > 0 ?
                    NS_PER_SEC / this.eventsPerSec : 0;
        }

        /**
         * Blocks for small amounts of time to achieve targetThroughput/events per sec
         *
         * @param events current events
         */
        void control(long events) {
            if (this.eventsPerSec <= 0) {
                return;
            }
            needSleep(events, System.currentTimeMillis());
        }

        /**
         * Blocks for small amounts of time to achieve targetThroughput/events per sec
         *
         * @param events current events
         * @param time   current time
         */
        void control(long events, long time) {
            if (this.eventsPerSec <= 0) {
                return;
            }
            needSleep(events, time);
        }

        private void needSleep(long events, long time) {
            float elapsedSec = (time - startTime) / 1000.f;

            if ((events / elapsedSec) < this.eventsPerSec) {
                return;
            }

            // control throughput / number of events by sleeping, on average,
            toSleepNs += sleepTimeNs;
            // If threshold reached, sleep a little
            if (toSleepNs >= MIN_SLEEP_NS) {
                long sleepStart = System.nanoTime();
                try {
                    final long sleepMs = toSleepNs / NS_PER_MS;
                    final long sleepNs = toSleepNs - sleepMs * NS_PER_MS;
                    Thread.sleep(sleepMs, (int) sleepNs);
                } catch (InterruptedException e) {
                    // will be taken care in finally block
                } finally {
                    // in case of short sleeps or oversleep ;adjust it for next sleep duration
                    final long sleptNS = System.nanoTime() - sleepStart;
                    if (sleptNS > 0) {
                        toSleepNs -= sleptNS;
                    } else {
                        toSleepNs = 0;
                    }
                }
            }
        }
    }
}
