/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.LockSupport;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * class for Performance statistics.
 */
public class PerfStats {
    final private String action;
    final private String csvFile;
    final private int messageSize;
    final private long windowInterval;
    final private ConcurrentLinkedQueue<TimeStamp> queue;
    final private ForkJoinPool executor;
    private QueueProcessor recorder;

    /**
     * private class for start and end time.
     */
    @NotThreadSafe
    final private class TimeStamp {
        final private long startTime;
        final private long endTime;
        final private int bytes;

        TimeStamp(long startTime, long endTime, int bytes) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.bytes = bytes;
        }

        TimeStamp(long endTime) {
            this.endTime = endTime;
            this.bytes = -1;
            this.startTime = -1;
        }

        private boolean isEnd() {
            return this.bytes == -1;
        }
    }


    public PerfStats(String action, int reportingInterval, int messageSize, String csvFile) {
        this.action = action;
        this.messageSize = messageSize;
        this.windowInterval = reportingInterval;
        this.csvFile = csvFile;
        this.queue = new ConcurrentLinkedQueue<TimeStamp>();
        this.executor = new ForkJoinPool(1);
        this.recorder = null;
    }

    /**
     * private class for start and end time.
     */
    @NotThreadSafe
    final private class QueueProcessor implements Runnable {
        final private LatencyWriter latencyRecorder;
        final private TimeWindow window;
        final private CountDownLatch latch;

        public QueueProcessor(String action, int massageSize, long startTime, String csvFile) throws IOException {
            this.latencyRecorder = csvFile == null ? new LatencyWriter(action, messageSize, startTime) :
                    new CSVLatencyWriter(action, messageSize, startTime, csvFile);
            this.window = new TimeWindow(startTime);
            this.latch = new CountDownLatch(1);
        }


        public void run() {
            TimeStamp t;
            while (true) {
                try {
                    t = queue.poll();
                    if (t != null) {
                        if (t.isEnd()) {
                            break;
                        }
                        record(t.startTime, t.endTime, t.bytes);
                    } else {
                        LockSupport.parkNanos(100);
                    }
                    print();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            latencyRecorder.printTotal(t.endTime);
            latch.countDown();
        }


        private void shutdown(long endTime) throws InterruptedException {
            queue.add(new TimeStamp(endTime));
            this.latch.await();
        }

        private void record(long startTime, long endTime, int bytes) {
            final int latency = (int) (endTime - startTime);
            window.record(bytes, latency);
            latencyRecorder.record(startTime, bytes, latency);
        }

        /**
         * print the performance statistics of current time window.
         */
        private void print() throws IOException {
            final long time = System.currentTimeMillis();
            if (window.windowTimeMS(time) >= windowInterval) {
                window.print(time);
                window.reset(time);
            }
        }

        /**
         * private class for Performance statistics within a given time window.
         */
        @NotThreadSafe
        private class TimeWindow {
            private long startTime;
            private long lastTime;
            private long count;
            private long bytes;
            private int maxLatency;
            private long totalLatency;

            public TimeWindow(long start) {
                reset(start);
            }

            public void reset(long start) {
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
             * @param bytes   number of bytes.
             * @param latency latency in ms.
             */
            private void record(long bytes, int latency) {
                this.count++;
                this.totalLatency += latency;
                this.bytes += bytes;
                this.maxLatency = Math.max(this.maxLatency, latency);
            }

            /**
             * print the window statistics
             */
            private void print(long time) {
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
            private long windowTimeMS(long time) {
                return time - window.startTime;
            }

            /**
             * get the time duration of this window
             */
            private long windowTimeMS() {
                return windowTimeMS(System.currentTimeMillis());
            }
        }

        private class LatencyWriter {
            final private static int MS_PER_SEC = 1000;
            final private static int MS_PER_MIN = MS_PER_SEC * 60;
            final private static int MS_PER_HR = MS_PER_MIN * 60;
            final private double[] percentiles = {0.5, 0.75, 0.95, 0.99, 0.999};

            final String action;
            final long start;
            final int messageSize;
            long count;
            long totalLatency;
            long maxLatency;
            long totalBytes;
            int[] latencies;
            ArrayList<LatencyRange> latencyRanges;


            private class LatencyRange {
                int latency;
                int start;
                int end;

                public LatencyRange(int latency, int start, int end) {
                    this.latency = latency;
                    this.start = start;
                    this.end = end;
                }
            }

            public LatencyWriter(String action, int messageSize, long start) {
                this.action = action;
                this.messageSize = messageSize;
                this.start = start;
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
                        latencyRanges.add(new LatencyRange(i, cur, cur + latencies[i]));
                        cur += latencies[i] + 1;
                        totalLatency += i * latencies[i];
                        count += latencies[i];
                        maxLatency = i;
                    }
                }
            }

            private int[] getPercentiles() {
                int[] percentileIds = new int[percentiles.length];
                int[] values = new int[percentileIds.length];
                int index = 0;

                for (int i = 0; i < percentiles.length; i++) {
                    percentileIds[i] = (int) (count * percentiles[i]);
                }

                for (LatencyRange lr : latencyRanges) {
                    while ((index < percentileIds.length) &&
                            (lr.start <= percentileIds[index]) && (percentileIds[index] <= lr.end)) {
                        values[index++] = lr.latency;
                    }
                }
                return values;
            }

            public void record(int bytes, int latency) {
                totalBytes += bytes;
                latencies[latency]++;
            }

            public void record(long start, int bytes, int latency) {
                this.record(bytes, latency);
            }

            public void printTotal(long endTime) {
                countLatencies();
                final double elapsed = (endTime - start) / 1000.0;
                final double recsPerSec = count / elapsed;
                final double mbPerSec = (this.totalBytes / (1024.0 * 1024.0)) / elapsed;
                int[] percs = getPercentiles();

                System.out.printf(
                        "%d records %s, %.3f records/sec, %d bytes record size, %.2f MB/sec, %.1f ms avg latency, %.1f ms max latency" +
                                ",%d ms 50th, %d ms 75th, %d ms 95th, %d ms 99th, %d ms 99.9th\n",
                        count, action, recsPerSec, messageSize, mbPerSec, totalLatency / ((double) count), (double) maxLatency,
                        percs[0], percs[1], percs[2], percs[3], percs[4]);
            }
        }

        @NotThreadSafe
        private class CSVLatencyWriter extends LatencyWriter {
            final private String csvFile;
            final private CSVPrinter csvPrinter;

            public CSVLatencyWriter(String action, int messageSize, long start, String csvFile) throws IOException {
                super(action, messageSize, start);
                this.csvFile = csvFile;
                csvPrinter = new CSVPrinter(Files.newBufferedWriter(Paths.get(csvFile)), CSVFormat.DEFAULT
                        .withHeader("Start Time (Milliseconds)", "event size (bytes)", action + "Latency (Milliseconds)"));
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
    }


    /**
     * start the performance statistics.
     *
     * @param startTime start time time
     */
    public synchronized void start(long startTime) throws IOException {
        if (this.recorder == null) {
            this.recorder = new QueueProcessor(action, messageSize, startTime, csvFile);
            executor.execute(this.recorder);
        }
    }


    /**
     * end the final performance statistics.
     *
     * @param endTime End time
     */
    public synchronized void shutdown(long endTime) throws InterruptedException {
        if (this.recorder != null) {
            recorder.shutdown(endTime);
            queue.clear();
            this.recorder = null;
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
        queue.add(new TimeStamp(startTime, endTime, bytes));
    }
}



