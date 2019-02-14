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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.ReinitializationRequiredException;

/**
 * class for Performance statistics.
 */
public class PerfStats {
    final private int messageSize;
    final private String action;
    final private Instant start;
    private Instant end;
    private long[] latencies;
    private int sampling;
    private int index;
    private long count;
    private long bytes;
    private long maxLatency;
    private long totalLatency;
    final private long windowInterval;
    private timeWindow window;
    private ReentrantLock lock;

    /**
     * private class for Performance statistics within a given time window.
     */
    private class timeWindow {
        final private Instant startTime;
        private Instant lastTime;
        private long count;
        private long bytes;
        private long maxLatency;
        private long totalLatency;

        public timeWindow() {
            this.startTime = Instant.now();
            this.lastTime = this.startTime;
            this.count = 0;
            this.bytes = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
        }

        /**
         * record the latency and bytes
         *
         * @param latency latency in ms.
         * @param bytes   number of bytes.
         */
        public void record(long latency, long bytes) {
            this.count++;
            this.totalLatency += latency;
            this.bytes += bytes;
            this.maxLatency = Math.max(this.maxLatency, latency);
        }

        /**
         * print the window statistics
         */
        public void print(Instant time) {
            this.lastTime = time;
            final double elapsed = Duration.between(this.startTime, this.lastTime).toMillis() / 1000.0;
            final double recsPerSec = count / elapsed;
            final double mbPerSec = (this.bytes / (1024.0 * 1024.0)) / elapsed;

            System.out.printf("%8d records %s, %9.1f records/sec, %6.2f MB/sec, %7.1f ms avg latency, %7.1f ms max latency\n",
                    count, action, recsPerSec, mbPerSec, totalLatency / (double) count, (double) maxLatency);
        }

        /**
         * get the current time duration of this window
         *
         * @param time current time.
         */
        public long windowTimeMS(Instant time) {
            return Duration.between(this.startTime, time).toMillis();
        }

        /**
         * get the time duration of this window
         */
        public long windowTimeMS() {
            return windowTimeMS(Instant.now());
        }
    }

    public PerfStats(String action, int reportingInterval, int messageSize, long numRecords) {
        this.action = action;
        this.start = Instant.now();
        this.end = this.start;
        this.count = 0;
        this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
        this.latencies = new long[(int) (numRecords / this.sampling) + 1];
        this.index = 0;
        this.maxLatency = 0;
        this.totalLatency = 0;
        this.windowInterval = reportingInterval;
        this.messageSize = messageSize;
        this.window = new timeWindow();
        this.lock = new ReentrantLock();
    }

    private void record(int bytes, Instant startTime, Instant endTime) {
        this.lock.lock();
        try {
            final long latency = Duration.between(startTime, endTime).toMillis();
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            window.record(latency, bytes);

            if (this.count % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
        } finally {
            this.lock.unlock();
        }
    }

    private static long[] percentiles(long[] latencies, int count, double... percentiles) {
        int size = Math.min(count, latencies.length);
        Arrays.sort(latencies, 0, size);
        long[] values = new long[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) (percentiles[i] * size);
            values[i] = latencies[index];
        }
        return values;
    }

    /**
     * print the performance statistics of current time window.
     */
    public void print() {
        this.lock.lock();
        try {
            final Instant time=Instant.now();
            if (window.windowTimeMS(time) >= windowInterval) {
                window.print(time);
                this.window = new timeWindow();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * print the final performance statistics.
     */
    public void printTotal(Instant endTime) {
        this.lock.lock();
        try {
            this.end = endTime;
            final double elapsed = Duration.between(start, end).toMillis() / 1000.0;
            final double recsPerSec = count / elapsed;
            final double mbPerSec = (this.bytes / (1024.0 * 1024.0)) / elapsed;

            long[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf(
                    "%d records %s, %.3f records/sec, %d bytes record size, %.2f MB/sec, %.1f ms avg latency, %.1f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                    count, action, recsPerSec, messageSize, mbPerSec, totalLatency / ((double) count), (double) maxLatency,
                    percs[0], percs[1], percs[2], percs[3]);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * record the data write/read time of given length of data.
     *
     * @param retVal    future to wait for.
     * @param startTime starting time
     * @param length    length of data read/written
     * @return a completable future for recording the end time.
     */
    public CompletableFuture recordTime(CompletableFuture retVal, Instant startTime, int length) {
        if (retVal == null) {
            final Instant endTime = Instant.now();
            record(length, startTime, endTime);
        } else {
            retVal = retVal.thenAccept((d) -> {
                final Instant endTime = Instant.now();
                record(length, startTime, endTime);
            });
        }
        return retVal;
    }

    /**
     * get the rate of events.
     *
     * @return rate of number of events till now.
     */
    public int eventsRate() {
        int rate = 0;
        this.lock.lock();
        try {
            final double elapsed = Duration.between(start, end).toMillis() / 1000.0;
            rate = (int) (count / elapsed);
        } finally {
            this.lock.unlock();
        }
        return rate;
    }
}
