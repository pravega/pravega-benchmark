/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.perf;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {

    ReaderWorker(int readerId, int eventsPerWorker, int secondsToRun, Instant start,
                 PerfStats stats, String readergrp, int timeout) {
        super(readerId, eventsPerWorker, secondsToRun,
                false, 0, start,
                stats, readergrp, timeout);
    }

    /**
     * read the data.
     *
     * @throws Exception if consumer failed to read required data.
     */
    public abstract String readData() throws Exception;

    /**
     * close the consumer/reader.
     */
    public abstract void close();

    /**
     * read all the data from reader/consumer.
     *
     * @param drainStats object which collects the details about the data read till there is no data to read from stream
     * @throws Exception if consumer failed to read required data.
     */
    public void cleanupEvents(PerfStats drainStats) throws Exception {
        String ret = null;
        do {
            final Instant startTime = Instant.now();
            ret = readData();
            if (ret != null) {
                drainStats.recordTime(null, startTime, ret.length());
            }
        } while (ret != null);
        drainStats.printTotal(Instant.now());
    }

    @Override
    public Void call() throws ExecutionException, Exception {
        String ret = null;
        try {

            for (int i = 0; (i < eventsPerWorker) &&
                    (Duration.between(StartTime, Instant.now()).getSeconds() < secondsToRun); i++) {
                ret = readData();
                if (ret != null) {
                    stats.recordTime(null, Instant.ofEpochMilli(Long.parseLong(ret.split(",")[0])), ret.length());
                }
            }
        } finally {
            close();
        }
        return null;
    }
}
