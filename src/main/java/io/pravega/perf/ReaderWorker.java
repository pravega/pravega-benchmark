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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * An Abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    final private static int MS_PER_SEC = 1000;
    final private Performance perf;

    ReaderWorker(int readerId, int events, int secondsToRun, long start,
                 PerfStats stats, String readergrp, int timeout, boolean writeAndRead) {
        super(readerId, events, secondsToRun, 0, start, stats, readergrp, timeout);

        perf = secondsToRun > 0 ? (writeAndRead ? new EventsTimeReaderRW() : new EventsTimeReader()) :
                (writeAndRead ? new EventsReaderRW() : new EventsReader());

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
                int i = 0;
                while (i < events) {
                    final long startTime = System.currentTimeMillis();
                    ret = readData();
                    if (ret != null) {
                        stats.recordTime(startTime, System.currentTimeMillis(), ret.length());
                        i++;
                    }
                }
            } finally {
                close();
            }
        }
    }

    private class EventsReaderRW implements Performance {
        public void benchmark() throws IOException {
            String ret = null;
            try {
                int i = 0;
                while (i < events) {
                    ret = readData();
                    if (ret != null) {
                        final long endTime = System.currentTimeMillis();
                        final long start = Long.parseLong(ret.substring(0, TIME_HEADER_SIZE));
                        stats.recordTime(start, endTime, ret.length());
                        i++;
                    }
                }
            } finally {
                close();
            }
        }
    }


    private class EventsTimeReader implements Performance {
        public void benchmark() throws IOException {
            final long msToRun = secondsToRun * MS_PER_SEC;
            String ret = null;
            long time = System.currentTimeMillis();

            try {
                while ((time - startTime) < msToRun) {
                    time = System.currentTimeMillis();
                    ret = readData();
                    if (ret != null) {
                        stats.recordTime(time, System.currentTimeMillis(), ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }

    private class EventsTimeReaderRW implements Performance {
        public void benchmark() throws IOException {
            final long msToRun = secondsToRun * MS_PER_SEC;
            String ret = null;
            long time = System.currentTimeMillis();
            try {
                while ((time - startTime) < msToRun) {
                    ret = readData();
                    time = System.currentTimeMillis();
                    if (ret != null) {
                        final long start = Long.parseLong(ret.substring(0, TIME_HEADER_SIZE));
                        stats.recordTime(startTime, time, ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }
}
