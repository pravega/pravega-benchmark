/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import java.util.function.Supplier;

class PerfStats {
    private int messageSize;
    private String action;
    private long windowStartTime;
    private long start;
    private long windowStart;
    private ArrayList<Double> latencies;
    private int iteration;
    private long count;
    private long bytes;
    private double maxLatency;
    private double totalLatency;
    private long windowCount;
    private long windowBytes;
    private long reportingInterval;

    public PerfStats(String action, int reportingInterval, int messageSize) {
           this.action = action;
           this.start = System.currentTimeMillis();
           this.windowStartTime = System.currentTimeMillis();
           this.windowStart = 0;
           this.iteration = 0;
           this.latencies = new  ArrayList<Double>();
           this.maxLatency = 0;
           this.totalLatency = 0;
           this.windowCount = 0;
           this.windowBytes = 0;
           this.reportingInterval = reportingInterval;
           this.messageSize = messageSize;
    }

    public synchronized void record(int bytes, long startTime, long endTime) {
        this.iteration++;
        this.windowBytes += bytes;
        this.windowCount++;        
        /* did we arrived at reporting time */
        if ((endTime - windowStartTime)  >= reportingInterval) {
            printWindow(endTime);
            newWindow(count);
        }
    }

    private void printWindow(long endTime) {
        long elapsed = endTime - windowStartTime;
        double latency=(double)(elapsed / (double) windowCount);
        double recsPerSec = 1000.0 * windowCount / (double) elapsed;
        double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);

        this.bytes += this.windowBytes;
        this.totalLatency += latency; 
        this.maxLatency = Math.max(this.maxLatency, latency); 
        this.latencies.add(latency);
        this.count++;
        
        System.out.printf("%8d records %s, %9.1f records/sec, %9.3f MB/sec, %7.4f ms avg latency.\n",
                           windowCount, action, recsPerSec, mbPerSec, latency);  
    }

    private void newWindow(long currentNumber) {
        this.windowStart = currentNumber;
        this.windowStartTime = System.currentTimeMillis();
        this.windowCount = 0;
        this.windowBytes = 0;
    }

    public synchronized void printAll() {
        /*
        for (int i = 0; i < latencies.length; i++) {
            System.out.printf("%d %d\n", i, latencies[i]);

        }
        */
    }

    public synchronized void printTotal(long endTime) {
        long elapsed = endTime - start;
        double recsPerSec = 1000.0 * iteration / (double) elapsed;
        double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
        //double[] percs = percentiles(this.latencies, 0.5, 0.95, 0.99, 0.999);
        System.out.printf(
                "%d records %s, %.3f records/sec, %d bytes record size, %.3f MB/sec, %.4f ms avg latency, %.4f ms max latency\n", 
                iteration, action, recsPerSec,  messageSize, mbPerSec, totalLatency / ((double) count), (double) maxLatency);
        /*  
        System.out.printf("latencies percentiles:  %.4f ms 50th, %.4f ms 95th, %.4f ms 99th, %.4f ms 99.9th.\n",
                           percs[0], percs[1], percs[2], percs[3]);
        */
        
    }

    private double[] percentiles(double[] latencies, double... percentiles) {
        Arrays.sort(latencies, 0, (int)count);
        double[] values = new double[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) (percentiles[i] * count);
            values[i] = latencies[index];
        }
        return values;
    }

    public CompletableFuture runAndRecordTime(Supplier<CompletableFuture> fn, long startTime, int length) throws Exception {
        CompletableFuture  retVal = fn.get();
        if(retVal == null) {
            final long endTime = System.currentTimeMillis();
            record(length,startTime,  endTime);
        } else {
            retVal = retVal.thenAccept((d) -> {
                final long endTime = System.currentTimeMillis();
                record(length,startTime, endTime);
            });
        }
        return retVal;

    }

    public CompletableFuture writeAndRecordTime(Supplier<CompletableFuture> fn, int length) throws Exception {
        CompletableFuture  retVal=null;
        final long startTime = System.currentTimeMillis();
 
        retVal = fn.get();
        
        if(retVal == null) {
            final long endTime = System.currentTimeMillis(); 
            record(length, startTime,  endTime);
        } else {
            retVal = retVal.thenAccept((d) -> {
                final long endTime = System.currentTimeMillis(); 
                record(length, startTime, endTime);
            });
        }
        return retVal;
    }

}
