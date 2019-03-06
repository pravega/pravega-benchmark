/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package com.dell.pravega.perf;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    final private performance perf;

    ReaderWorker(int readerId, int events, int secondsToRun, long start,
                 PerfStats stats, String readergrp, int timeout) {
        super(readerId, events, secondsToRun,
                 0, start, stats, readergrp, timeout);
        perf = secondsToRun > 0 ? new eventstimeReader() : new eventsReader();
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

    private class eventsReader implements performance {

        public void benchmark() throws IOException {
            String ret = null;
            try {
                for (int i = 0; i < events; i++) {
                    final long startTime = System.currentTimeMillis();
                    ret = readData();
                    if (ret != null) {
                        stats.recordTime(startTime, System.currentTimeMillis(), ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }

    private class eventstimeReader implements performance {
        public void benchmark() throws IOException {
            String ret = null;
            try {

                while (((System.currentTimeMillis() - StartTime) / 1000) < secondsToRun) {
                    final long startTime = System.currentTimeMillis();
                    ret = readData();
                    if (ret != null) {
                        stats.recordTime(startTime, System.currentTimeMillis(), ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }
}
