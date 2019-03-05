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
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;


/**
 * class for Performance statistics.
 */
public class PerfStats {
    final private int messageSize;
    final private String action;
    final private long start;
    private long[] latencies;
    private int sampling;
    private int index;
    private long count;
    private long bytes;
    private long maxLatency;
    private long totalLatency;
    final private long windowInterval;
    private TimeWindow window;
    final private CSVPrinter printer;
    final private ConcurrentLinkedQueue<TimeStamp> queue;
    final private ForkJoinPool executor;


    /**
     * private class for start and end time.
     */
    private class TimeStamp {
        final private int bytes;
        final private long start;
        final private long end;

        TimeStamp(int bytes, long start, long end) {
            this.bytes = bytes;
            this.start = start;
            this.end = end;
        }
    }

    /**
     * private class for Performance statistics within a given time window.
     */
    private class TimeWindow {
        final private long startTime;
        private long lastTime;
        private long count;
        private long bytes;
        private long maxLatency;
        private long totalLatency;

        public TimeWindow(long start) {
            this.startTime = start;
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
        public void print(long time) {
            this.lastTime = time;
            final double elapsed = (this.lastTime - this.startTime) / 1000.0;
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
        public long windowTimeMS(long time) {
            return time - window.startTime;
        }

        /**
         * get the time duration of this window
         */
        public long windowTimeMS() {
            return windowTimeMS(System.currentTimeMillis());
        }
    }

    public PerfStats(String action, int reportingInterval, int messageSize, long numRecords, String csvFile) throws IOException {
        this.action = action;
        this.start = System.currentTimeMillis();
        this.count = 0;
        this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
        this.latencies = new long[(int) (numRecords / this.sampling) + 1];
        this.index = 0;
        this.maxLatency = 0;
        this.totalLatency = 0;
        this.windowInterval = reportingInterval;
        this.messageSize = messageSize;
        this.window = new TimeWindow(this.start);
        this.queue = new ConcurrentLinkedQueue<TimeStamp>();
        this.executor = new ForkJoinPool(1);
        if (csvFile != null) {
            this.printer = new CSVPrinter(Files.newBufferedWriter(Paths.get(csvFile)), CSVFormat.DEFAULT
                    .withHeader("event size (bytes)", "Start Time (Milliseconds)", action + " Latency (Milliseconds)"));
        } else {
            this.printer = null;
        }
        executor.execute(new ProcessQueue());
    }

    /**
     * private class for start and end time.
     */
    private class ProcessQueue implements Runnable {
        public void run() {
            TimeStamp t;
            while (true) {
                try {
                    t = queue.poll();
                    if (t != null) {
                        record(t.bytes, t.start, t.end);
                    }
                    print();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


    private void record(int bytes, long startTime, long endTime) {
        final long latency = endTime - startTime;
        this.count++;
        this.bytes += bytes;
        this.totalLatency += latency;
        this.maxLatency = Math.max(this.maxLatency, latency);
        window.record(latency, bytes);

        if (this.count % this.sampling == 0) {
            this.latencies[index] = latency;
            this.index++;
        }


        if (this.printer != null) {
            try {
                printer.printRecord(bytes, startTime, latency);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
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
    private void print() throws IOException {
        final long time = System.currentTimeMillis();
        if (window.windowTimeMS(time) >= windowInterval) {
            window.print(time);
            this.window = new TimeWindow(time);
            if (printer != null) {
                printer.flush();
            }
        }
    }

    /**
     * print the final performance statistics.
     */
    public void printTotal(long endTime) {
        executor.shutdownNow();

        final double elapsed = (endTime - start) / 1000.0;
        final double recsPerSec = count / elapsed;
        final double mbPerSec = (this.bytes / (1024.0 * 1024.0)) / elapsed;

        long[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
        System.out.printf(
                "%d records %s, %.3f records/sec, %d bytes record size, %.2f MB/sec, %.1f ms avg latency, %.1f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                count, action, recsPerSec, messageSize, mbPerSec, totalLatency / ((double) count), (double) maxLatency,
                percs[0], percs[1], percs[2], percs[3]);
        if (printer != null) {
            try {
                printer.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * record the data write/read time of data.
     *
     * @param startTime starting time
     * @param endTime   End time
     * @param bytes     number of bytes written or read
     **/
    public void recordTime(long startTime, long endTime, int bytes) {
        queue.add(new TimeStamp(bytes, startTime, endTime));
    }
}


