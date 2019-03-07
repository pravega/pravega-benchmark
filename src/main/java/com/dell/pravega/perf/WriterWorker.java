/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.dell.pravega.perf;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * abstract class for Writers.
 */
public abstract class WriterWorker extends Worker implements Callable<Void> {
    final private performance perf;
    final private EventsController eCnt;

    WriterWorker(int sensorId, int events, int secondsToRun,
                 boolean isRandomKey, int messageSize, long start,
                 PerfStats stats, String streamName, int eventsPerSec, boolean wNr) {

        super(sensorId, events, secondsToRun,
                messageSize, start, stats,
                streamName, 0);
        this.eCnt = new EventsController(start, eventsPerSec);
        perf = secondsToRun > 0 ? (wNr ? new EventsWriterTimeRW() : new EventsWriterTime()) :
                (wNr ? new EventsWriterRW() : new EventsWriter());
    }

    /**
     * writes the data and benchmark
     *
     * @param data   data to write
     * @param record to call for benchmarking
     * @return time return the data sent time
     */
    public abstract long recordWrite(String data, TriConsumer record);

    /**
     * writes the data and benchmark
     *
     * @param data data to write
     */
    public abstract void writeData(String data);


    /**
     * flush the producer data.
     */
    public abstract void flush();

    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        perf.benchmark();
        return null;
    }

    private class EventsWriter implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            // Construct event payload
            String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
            String payload = String.format("%-" + messageSize + "s", val);

            for (int i = 0; i < events; i++) {
                recordWrite(payload, stats::recordTime);
                eCnt.control(i);
            }
            flush();
        }
    }

    private class EventsWriterRW implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            for (int i = 0; i < events; i++) {
                // Construct event payload
                String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
                String payload = String.format("%-" + messageSize + "s", val);
                writeData(payload);
                eCnt.control(i);
            }

            flush();

        }
    }


    private class EventsWriterTime implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            // Construct event payload
            String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
            String payload = String.format("%-" + messageSize + "s", val);
            long time = System.currentTimeMillis();

            for (int i = 0; ((time - StartTime) / 1000) < secondsToRun; i++) {
                time = recordWrite(payload, stats::recordTime);
                eCnt.control(i);
            }

            flush();
        }
    }

    private class EventsWriterTimeRW implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            long time = System.currentTimeMillis();
            for (int i = 0; ((time - StartTime) / 1000) < secondsToRun; i++) {
                time = System.currentTimeMillis();
                String val = time + ", " + workerID + ", " + (int) (Math.random() * 200);
                String payload = String.format("%-" + messageSize + "s", val);
                writeData(payload);
                eCnt.control(i);
            }

            flush();
        }
    }


}
