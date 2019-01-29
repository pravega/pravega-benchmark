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
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.time.Instant;
import java.time.Duration;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.ReinitializationRequiredException;

/**
 *  class for Performance statistics.
 */
public class PerfStats {
    final private int messageSize;
    final private String action;
    private Instant windowStartTime;
    final private Instant start;
    private long[] latencies;
    private int sampling;
    private int index;
    private long count;
    private long bytes;
    private long maxLatency;
    private long totalLatency;
    private long windowMaxLatency;
    private long windowTotalLatency;
    private long windowCount;
    private long windowBytes;
    final private long reportingInterval;

    public PerfStats(String action, int reportingInterval, int messageSize, long numRecords) {
        this.action = action;
        this.start = Instant.now();
        this.windowStartTime = Instant.now();
        this.count = 0;
        this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
        this.latencies = new long[(int) (numRecords / this.sampling) + 1];
        this.index = 0;
        this.maxLatency = 0;
        this.totalLatency = 0;
        this.windowCount = 0;
        this.windowBytes = 0;
        this.reportingInterval = reportingInterval;
        this.messageSize = messageSize;
    }

    private synchronized void record(int bytes, Instant startTime, Instant endTime) {
        final long latency = Duration.between(startTime, endTime).toMillis();
        this.count++;
        this.windowCount++;
        this.bytes += bytes;
        this.windowBytes += bytes;
        this.totalLatency += latency;
        this.windowTotalLatency += latency;
        this.maxLatency = Math.max(this.maxLatency, latency);
        this.windowMaxLatency = Math.max(windowMaxLatency, latency);

        if (this.count % this.sampling == 0) {
            this.latencies[index] = latency;
            this.index++;
        }

        /* did we arrived at reporting time */
        if (Duration.between(windowStartTime, endTime).toMillis() >= reportingInterval) {
            printWindow(endTime);
            newWindow();
        }
    }

    private void printWindow(Instant endTime) {
        final long elapsed = Duration.between(windowStartTime, endTime).getSeconds();
        final double recsPerSec = windowCount / (double) elapsed;
        final double mbPerSec = (this.windowBytes / (1024.0 * 1024.0))/(double) elapsed;

        System.out.printf("%8d records %s, %9.1f records/sec, %6.2f MB/sec, %7.1f ms avg latency, %7.1f ms max latency\n",
                windowCount, action, recsPerSec, mbPerSec,
                windowTotalLatency / (double) windowCount,
                (double) windowMaxLatency);
    }

    private void newWindow() {
        this.windowStartTime = Instant.now();
        this.windowCount = 0;
        this.windowBytes = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
    }


    /**
     * print the final performance statistics.
     *
     * @param endTime        endtime to performance benchmarking.
     */
    public synchronized void printTotal(Instant endTime) {
        final long elapsed = Duration.between(start, endTime).getSeconds();
        double recsPerSec =  count / (double) elapsed;
        double mbPerSec = (this.bytes / (1024.0 * 1024.0))/(double) elapsed ;

        long[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
        System.out.printf(
                "%d records %s, %.3f records/sec, %d bytes record size, %.2f MB/sec, %.1f ms avg latency, %.1f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                count, action, recsPerSec, messageSize, mbPerSec, totalLatency / ((double) count), (double) maxLatency,
                percs[0], percs[1], percs[2], percs[3]);
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
     * record the data write/read time of given length of data.
     *
     * @param retVal         future to wait for.
     * @param startTime      starting time
     * @param length         length of data read/written
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
}
