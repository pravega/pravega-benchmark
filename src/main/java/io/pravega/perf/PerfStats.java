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
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Class for Performance statistics.
 */
public class PerfStats {
    private static Logger log = LoggerFactory.getLogger(PerfStats.class);

    private static final double[] PERCENTILES = {0.5, 0.75, 0.95, 0.99, 0.999, 0.9999};

    final private String action;
    final private String csvFile;
    final private String throughputCsvFile;
    final private int messageSize;
    final private int windowInterval;
    final private ConcurrentLinkedQueue<TimeStamp> queue;
    final private ForkJoinPool executor;

    @GuardedBy("this")
    private Future<Void> ret;

    /**
     * Private class for start and end time.
     */
    final static private class TimeStamp {
        final private long startTime;
        final private long endTime;
        final private int bytes;

        private TimeStamp(long startTime, long endTime, int bytes) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.bytes = bytes;
        }

        private TimeStamp(long endTime) {
            this(-1, endTime, -1);
        }

        private boolean isEnd() {
            return this.bytes == -1 && this.startTime == -1;
        }
    }

    public PerfStats(String action, int reportingInterval, int messageSize, String csvFile, String throughputCsvFile) {
        this.action = action;
        this.messageSize = messageSize;
        this.windowInterval = reportingInterval;
        this.csvFile = csvFile;
        this.throughputCsvFile = throughputCsvFile;
        this.queue = new ConcurrentLinkedQueue<>();
        this.executor = new ForkJoinPool(1);
        this.ret = null;
    }

    /**
     * Private class for start and end time.
     */
    final private class QueueProcessor implements Callable {
        final private static int NS_PER_MICRO = 1000;
        final private static int MICROS_PER_MS = 1000;
        final private static int NS_PER_MS = NS_PER_MICRO * MICROS_PER_MS;
        final private static int PARK_NS = NS_PER_MICRO;
        final private long startTime;
        final TimeWindow window;
        CSVThroughputWriter throughputRecorder;

        private QueueProcessor(long startTime){
            this.startTime = startTime;
            window = new TimeWindow(action, startTime);
        }

        public Void call() throws IOException {
            final LatencyWriter latencyRecorder = csvFile == null ? new LatencyWriter(action, messageSize, startTime) :
                    new CSVLatencyWriter(action, messageSize, startTime, csvFile);
            final int minWaitTimeMS = windowInterval / 50;
            final long totalIdleCount = (NS_PER_MS / PARK_NS) * minWaitTimeMS;
            boolean doWork = true;
            long time = startTime;
            long idleCount = 0;

            throughputRecorder = throughputCsvFile != null ? new CSVThroughputWriter(action, throughputCsvFile) : null;
            TimeStamp t;

            while (doWork) {
                t = queue.poll();
                if (t != null) {
                    if (t.isEnd()) {
                        doWork = false;
                    } else {
                        final int latency = (int) (t.endTime - t.startTime);
                        window.record(t.bytes, latency);

                        latencyRecorder.record(t.startTime, t.bytes, latency);
                    }
                    time = t.endTime;
                    if (window.windowTimeMS(time) > windowInterval) {
                        resetWindow(time);
                    }
                } else {
                    LockSupport.parkNanos(PARK_NS);
                    idleCount++;
                    if (idleCount > totalIdleCount) {
                        time = System.currentTimeMillis();
                        idleCount = 0;
                        if (window.windowTimeMS(time) > windowInterval) {
                            resetWindow(time);
                        }
                    }
                }
            }

            latencyRecorder.printTotal(time);
            if (throughputRecorder != null) {
                throughputRecorder.close();
            }
            return null;
        }

        private final void resetWindow(long time) {
            window.lastTime(time);

            if (throughputRecorder != null) {
                throughputRecorder.record(window.startTime, window.lastTime, window.bytes, window.getMBPerSecond(), window.count, window.getEventsPerSecond());
            }

            window.print();
            window.reset(time);
        }
    }

    /**
     * Private class for Performance statistics within a given time window.
     */
    @NotThreadSafe
    final static private class TimeWindow {
        final private String action;
        private long startTime;
        private long lastTime;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private double elapsedSeconds = 0;

        private TimeWindow(String action, long start) {
            this.action = action;
            reset(start);
        }

        private void reset(long start) {
            this.startTime = start;
            this.lastTime = this.startTime;
            this.count = 0;
            this.bytes = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.elapsedSeconds = 0;
        }

        /**
         * Record the latency and bytes
         *
         * @param bytes   number of bytes.
         * @param latency latency in ms.
         */
        private void record(long bytes, int latency) {
            this.count++;
            this.totalLatency += latency;
            this.bytes += bytes;
            this.maxLatency = Math.max(this.maxLatency, latency);
        }

        public void lastTime(long time) {
            this.lastTime = time;

            assert this.lastTime > this.startTime : "Invalid Start and EndTime";
            this.elapsedSeconds = (this.lastTime - this.startTime) / 1000.0;
        }

        public double getMBPerSecond() {
            assert this.elapsedSeconds > 0 : "Elapsed Seconds cannot be zero";

            return (this.bytes / (1024.0 * 1024.0)) / elapsedSeconds;
        }

        public double getEventsPerSecond() {
            assert this.elapsedSeconds > 0 : "Elapsed Seconds cannot be zero";

            return count/elapsedSeconds;
        }

        /**
         * Print the window statistics
         */
        private void print() {
            assert this.lastTime > this.startTime : "Invalid Start and EndTime";
            final double recsPerSec = count / elapsedSeconds;
            final double mbPerSec = getMBPerSecond();

            log.info(String.format("%8d records %s, %9.1f records/sec, %6.2f MiB/sec, %7.1f ms avg latency, %7.1f ms max latency",
                    count, action, recsPerSec, mbPerSec, totalLatency / (double) count, (double) maxLatency));
        }

        /**
         * Get the current time duration of this window
         *
         * @param time current time.
         */
        private long windowTimeMS(long time) {
            return time - startTime;
        }
    }

    /***
     * Perf Recorder that records the number of bytes every second.  Percentiles are reported in human form, i.e. MB/s
     */
    @NotThreadSafe
    static private class CSVThroughputWriter  {
        final private CSVPrinter csvPrinter;

        CSVThroughputWriter(String action, String csvFile) throws IOException {
            csvPrinter = new CSVPrinter(Files.newBufferedWriter(Paths.get(csvFile)), CSVFormat.DEFAULT
                .withHeader("Start", "End","Events",action+" Events Throughput", "Bytes", action+" MiB Throughput"));
        }

        public void record(long startTime, long lastTime, long bytes, double mbPerSecond, long count, double eventsPerSecond) {
            try {
                csvPrinter.printRecord(startTime, lastTime, count, Precision.round(eventsPerSecond, 2), bytes, Precision.round(mbPerSecond, 2));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public void close() {
            try {
                csvPrinter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @NotThreadSafe
    static private class LatencyWriter {
        final static int MS_PER_SEC = 1000;
        final static int MS_PER_MIN = MS_PER_SEC * 60;
        final static int MS_PER_HR = MS_PER_MIN * 60;
        final String action;
        final int messageSize;
        final long startTime;
        final int[] latencies;
        long count;
        long totalLatency;
        long maxLatency;
        long totalBytes;
        ArrayList<int[]> latencyRanges;

        LatencyWriter(String action, int messageSize, long startTime) {
            this.action = action;
            this.messageSize = messageSize;
            this.startTime = startTime;
            this.latencies = new int[MS_PER_HR];
            this.latencyRanges = null;
            this.totalLatency = 0;
            this.maxLatency = 0;
            this.count = 0;
        }

        private void countLatencies() {
            count = 0;
            latencyRanges = new ArrayList<>();
            for (int i = 0, cur = 0; i < latencies.length; i++) {
                if (latencies[i] > 0) {
                    latencyRanges.add(new int[]{cur, cur + latencies[i], i});
                    cur += latencies[i] + 1;
                    totalLatency += i * latencies[i];
                    count += latencies[i];
                    maxLatency = i;
                }
            }
        }

        private int[] getPercentiles() {
            int[] percentileIds = new int[PERCENTILES.length];
            int[] values = new int[percentileIds.length];
            int index = 0;

            for (int i = 0; i < PERCENTILES.length; i++) {
                percentileIds[i] = (int) (count * PERCENTILES[i]);
            }

            for (int[] lr : latencyRanges) {
                while ((index < percentileIds.length) &&
                        (lr[0] <= percentileIds[index]) && (percentileIds[index] <= lr[1])) {
                    values[index++] = lr[2];
                }
            }
            return values;
        }

        public void record(int bytes, int latency) {
            assert latency < latencies.length : "Invalid latency";
            totalBytes += bytes;
            latencies[latency]++;
        }

        public void record(long start, int bytes, int latency) {
            this.record(bytes, latency);
        }

        public void printTotal(long endTime) {
            countLatencies();
            final double elapsed = (endTime - startTime) / 1000.0;
            final double recsPerSec = count / elapsed;
            final double mbPerSec = (this.totalBytes / (1024.0 * 1024.0)) / elapsed;
            int[] percs = getPercentiles();

            log.info(String.format(
                    "%d records %s, %.3f records/sec, %d bytes record size, %.2f MiB/sec, %.1f ms avg latency, %.1f ms max latency" +
                            ", %d ms 50th, %d ms 75th, %d ms 95th, %d ms 99th, %d ms 99.9th, %d ms 99.99th.",
                    count, action, recsPerSec, messageSize, mbPerSec, totalLatency / ((double) count), (double) maxLatency,
                    percs[0], percs[1], percs[2], percs[3], percs[4], percs[5]));
        }
    }

    @NotThreadSafe
    static private class CSVLatencyWriter extends LatencyWriter {
        final private String csvFile;
        final private CSVPrinter csvPrinter;

        CSVLatencyWriter(String action, int messageSize, long start, String csvFile) throws IOException {
            super(action, messageSize, start);
            this.csvFile = csvFile + "-latency.csv";;
            csvPrinter = new CSVPrinter(Files.newBufferedWriter(Paths.get(csvFile)), CSVFormat.DEFAULT
                    .withHeader("Start Time (Milliseconds)", "event size (bytes)", action + " Latency (Milliseconds)"));
        }

        private void readCSV() {
            try {
                CSVParser csvParser = new CSVParser(Files.newBufferedReader(Paths.get(csvFile)), CSVFormat.DEFAULT
                        .withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());

                for (CSVRecord csvEntry : csvParser) {
                    record(Integer.parseInt(csvEntry.get(1)), Integer.parseInt(csvEntry.get(2)));
                }
                csvParser.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void record(long start, int bytes, int latency) {
            try {
                csvPrinter.printRecord(start, bytes, latency);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void printTotal(long endTime) {
            try {
                csvPrinter.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            readCSV();
            super.printTotal(endTime);
        }
    }

    /**
     * Start the performance statistics.
     *
     * @param startTime start time time
     */
    public synchronized void start(long startTime) {
        if (this.ret == null) {
            this.ret = executor.submit(new QueueProcessor(startTime));
        }
    }

    /**
     * End the final performance statistics.
     *
     * @param endTime End time
     * @throws ExecutionException   If an exception occurred.
     * @throws InterruptedException If an exception occurred.
     */
    public synchronized void shutdown(long endTime) throws ExecutionException, InterruptedException {
        if (this.ret != null) {
            queue.add(new TimeStamp(endTime));
            ret.get();
            executor.shutdownNow();
            queue.clear();
            this.ret = null;
        }
    }

    /**
     * Record the data write/read time of data.
     *
     * @param startTime starting time
     * @param endTime   End time
     * @param bytes     number of bytes written or read
     **/
    public void recordTime(long startTime, long endTime, int bytes) {
        queue.add(new TimeStamp(startTime, endTime, bytes));
    }
}
