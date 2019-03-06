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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    final private performance perf;

    ReaderWorker(int readerId, int events, int secondsToRun, long start,
                 PerfStats stats, String readergrp, int timeout, boolean wNr) {
        super(readerId, events, secondsToRun,
                0, start, stats, readergrp, timeout);

        perf = secondsToRun > 0 ? (wNr ? new EventsTimeReaderRW() : new EventsTimeReader()) :
                (wNr ? new EventsReaderRW() : new EventsReader());

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

    private class EventsReader implements performance {

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

    private class EventsReaderRW implements performance {
        public void benchmark() throws IOException {
            String ret = null;
            try {
                for (int i = 0; i < events; i++) {
                    ret = readData();
                    if (ret != null) {
                        final long endTime = System.currentTimeMillis();
                        final long startTime = Long.parseLong(ret.split(",")[0]);
                        stats.recordTime(startTime, endTime, ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }


    private class EventsTimeReader implements performance {
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

    private class EventsTimeReaderRW implements performance {
        public void benchmark() throws IOException {
            String ret = null;
            try {

                while (((System.currentTimeMillis() - StartTime) / 1000) < secondsToRun) {
                    ret = readData();
                    if (ret != null) {
                        final long endTime = System.currentTimeMillis();
                        final long startTime = Long.parseLong(ret.split(",")[0]);
                        stats.recordTime(startTime, endTime, ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }
}
