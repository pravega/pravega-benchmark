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
                 PerfStats stats, String streamName, int eventsPerSec) {

        super(sensorId, events, secondsToRun,
                messageSize, start, stats,
                streamName, 0);
        this.eCnt = new EventsController(start, eventsPerSec);
        perf = secondsToRun > 0 ? new EventsWriterTime() : new EventsWriter();
    }

    /**
     * writes the data and benchmark
     *
     * @param data   data to write
     * @param record to call for benchmarking
     */
    public abstract void recordWrite(String data, TriConsumer record);

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

    private class EventsWriterTime implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            // Construct event payload
            String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
            String payload = String.format("%-" + messageSize + "s", val);

            for (int i = 0; ((System.currentTimeMillis() - StartTime) / 1000) < secondsToRun; i++) {
                recordWrite(payload, stats::recordTime);
                eCnt.control(i);
            }

            flush();
        }
    }
}
