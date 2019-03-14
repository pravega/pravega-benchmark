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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    final private Performance perf;

    ReaderWorker(int readerId, int events, int secondsToRun, Instant start,
                 PerfStats stats, String readergrp, int timeout) {
        super(readerId, events, secondsToRun,
                false, 0, start,
                stats, readergrp, timeout);
        perf = secondsToRun > 0 ? new EventstimeReader() : new EventsReader();
    }

    /**
     * read the data.
     */
    public abstract String readData();

    /**
     * close the consumer/reader.
     */
    public abstract void close();

    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        perf.benchmark();
        return null;
    }

    private class EventsReader implements Performance {

        public void benchmark() throws IOException {
            String ret = null;
            try {
                for (int i = 0; i < events; i++) {
                    final Instant startTime = Instant.now();
                    ret = readData();
                    if (ret != null) {
                        stats.recordTime(null, startTime, ret.length());
                    }
                    stats.print();
                }
            } finally {
                close();
            }
        }
    }

    private class EventstimeReader implements Performance {
        public void benchmark() throws IOException {
            String ret = null;
            try {

                for (int i = 0; Duration.between(StartTime, Instant.now()).getSeconds() < secondsToRun; i++) {
                    final Instant startTime = Instant.now();
                    ret = readData();
                    if (ret != null) {
                        stats.recordTime(null, startTime, ret.length());
                    }
                    stats.print();
                }
            } finally {
                close();
            }
        }
    }
}
