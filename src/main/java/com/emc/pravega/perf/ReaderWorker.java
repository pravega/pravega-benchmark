/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.perf;

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
                false, 0, start,
                stats, readergrp, timeout);
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
                        stats.recordTime(null, startTime, ret.length());
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
                        stats.recordTime(null, startTime, ret.length());
                    }
                }
            } finally {
                close();
            }
        }
    }
}
