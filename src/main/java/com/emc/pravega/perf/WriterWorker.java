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
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * abstract class for Writers.
 */
public abstract class WriterWorker extends Worker implements Callable<Void> {
    final private performance perf;
    final private ThroughputController tput;

    WriterWorker(int sensorId, int events, int secondsToRun,
                 boolean isRandomKey, int messageSize, Instant start,
                 PerfStats stats, String streamName, ThroughputController tput) {

        super(sensorId, events, secondsToRun,
                isRandomKey, messageSize, start,
                stats, streamName, 0);
        this.tput = tput;
        perf = secondsToRun > 0 ? new throughputWriter() : new eventsWriter();
    }

    /**
     * writes the data.
     *
     * @param key  key for data.
     * @param data data to write
     */
    public abstract CompletableFuture writeData(String key, String data);

    /**
     * flush the producer data.
     */
    public abstract void flush();

    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        perf.benchmark();
        return null;
    }

    private class eventsWriter implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            CompletableFuture retFuture = null;
            Random rand = new Random();

            for (int i = 0; i < events; i++) {

                // Construct event payload
                String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
                String payload = String.format("%-" + messageSize + "s", val);
                String key;
                if (isRandomKey) {
                    key = Integer.toString(workerID + rand.nextInt());
                } else {
                    key = Integer.toString(workerID);
                }

                final Instant startTime = Instant.now();
                retFuture = writeData(key, payload);
                // event ingestion
                retFuture = stats.recordTime(retFuture, startTime, payload.length());
                tput.control(stats.eventsRate());

            }

            flush();

            //Wait for the last packet to get acked
            retFuture.get();
        }
    }

    private class throughputWriter implements performance {

        public void benchmark() throws InterruptedException, ExecutionException, IOException {
            CompletableFuture retFuture = null;
            Random rand = new Random();

            while (Duration.between(StartTime, Instant.now()).getSeconds() < secondsToRun) {
                // Construct event payload
                String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
                String payload = String.format("%-" + messageSize + "s", val);
                String key;
                if (isRandomKey) {
                    key = Integer.toString(workerID + rand.nextInt());
                } else {
                    key = Integer.toString(workerID);
                }

                final Instant beginTime = Instant.now();
                retFuture = writeData(key, payload);
                // event ingestion
                retFuture = stats.recordTime(retFuture, beginTime, payload.length());
                tput.control(stats.eventsRate());
            }

            flush();

            //Wait for the last packet to get acked
            retFuture.get();
        }
    }
}
