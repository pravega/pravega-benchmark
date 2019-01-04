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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.time.Instant;
import java.time.Duration;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.TxnFailedException;
import java.util.concurrent.atomic.AtomicInteger;

public class PravegaWriterWorker extends Worker implements Callable<Void> {
    public final static AtomicInteger eventCount = new AtomicInteger(0);
    final EventStreamWriter<String> producer;

    PravegaWriterWorker(int sensorId, int eventsPerSec, int secondsToRun,
                        boolean isRandomKey, int messageSize, Instant start,
                        PerfStats stats, String streamName, long totalEvents,
                        ClientFactory factory) {

        super(sensorId, eventsPerSec, secondsToRun,
                isRandomKey, messageSize, start,
                stats, streamName, totalEvents, 0);

        this.producer = factory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * This function will be executed in a loop and time behavior is measured.
     *
     * @return A function which takes String key and data and returns a future object.
     */
    public CompletableFuture writeData(String key, String data) throws TxnFailedException, IllegalStateException {
        return producer.writeEvent(key, data);
    }

    @Override
    public Void call() throws TxnFailedException, InterruptedException, ExecutionException, IllegalStateException {
        CompletableFuture retFuture = null;

        do {
            final Instant loopStartTime = Instant.now();
            for (int i = 0; (i < eventsPerSec) && (eventCount.incrementAndGet() <= totalEvents) &&
                    (Duration.between(loopStartTime, Instant.now()).getSeconds() < 1); i++) {

                // Construct event payload
                String val = System.currentTimeMillis() + ", " + workerID + ", " + (int) (Math.random() * 200);
                String payload = String.format("%-" + messageSize + "s", val);
                String key;
                if (isRandomKey) {
                    key = Integer.toString(workerID + new Random().nextInt());
                } else {
                    key = Integer.toString(workerID);
                }

                final Instant startTime = Instant.now();
                retFuture = writeData(key, payload);
                // event ingestion
                retFuture = stats.recordTime(retFuture, startTime, payload.length());
            }

            long timeSpent = Duration.between(loopStartTime, Instant.now()).toMillis();
            if (timeSpent < 1000) {
                Thread.sleep(1000 - timeSpent);
            }
        } while ((Duration.between(StartTime, Instant.now()).getSeconds() < secondsToRun) &&
                (eventCount.get() < totalEvents));

        producer.flush();
        // producer.close();

        //Wait for the last packet to get acked
        retFuture.get();

        return null;
    }
}