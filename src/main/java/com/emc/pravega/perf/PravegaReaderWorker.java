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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;

public class PravegaReaderWorker implements Callable<Void> {
    public final static AtomicInteger eventCount = new AtomicInteger(0);
    public final long totalEvents;
    private final EventStreamReader<String> reader;
    private final int secondsToRun;
    private final long StartTime;
    private final int timeout;
    private final PerfStats stats;
    private final String readerId;

    PravegaReaderWorker(int readerId, int secondsToRun, long start,
                        ClientFactory factory, PerfStats stats,
                        String readergrp, int timeout, long totalEvents) {
        this.readerId = Integer.toString(readerId);
        this.secondsToRun = secondsToRun;
        this.StartTime = start;
        this.stats = stats;
        this.timeout = timeout;
        this.totalEvents = totalEvents;

        reader = factory.createReader(
            this.readerId, readergrp, new UTF8StringSerializer(), ReaderConfig.builder().build());
    }

    public void cleanupEvents(PerfStats drainStats) throws ReinitializationRequiredException {
        EventRead<String> event;
        String ret = null;
        do {
            long startTime = System.currentTimeMillis();
            event = reader.readNextEvent(timeout);
            ret = event.getEvent();
            if (ret != null) {
                drainStats.recordTime(null, startTime, ret.length());
            }
        } while (ret != null);
        drainStats.printTotal(System.currentTimeMillis());
    }

    @Override
    public Void call() throws ReinitializationRequiredException, ExecutionException {
        EventRead<String> event = null;
        String ret = null;
        final long mSeconds = secondsToRun * 1000;
        long diffTime = mSeconds;
        try {
            do {
                event = reader.readNextEvent(timeout);
                ret = event.getEvent();
                if (ret != null) {
                    stats.recordTime(null, Long.parseLong(ret.split(",")[0]), ret.length());
                }
                diffTime = System.currentTimeMillis() - StartTime;
            } while ((eventCount.incrementAndGet() < totalEvents) && (diffTime < mSeconds));
        } finally {
            reader.close();
        }
        return null;
    }
}
