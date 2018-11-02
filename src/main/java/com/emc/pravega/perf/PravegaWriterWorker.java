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
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.TxnFailedException;

public class PravegaWriterWorker implements Callable<Void> {
    final EventStreamWriter<String> producer;
    private final int producerId;
    private final int eventsPerSec;
    private final int secondsToRun;
    private final int messageSize;
    private final long StartTime;
    private final boolean isRandomKey;
    private final PerfStats stats;

    PravegaWriterWorker(int sensorId, int eventsPerSec, int secondsToRun,
                        boolean isRandomKey, int messageSize, long start,
                        ClientFactory factory, PerfStats stats, String streamName) {
        this.producerId = sensorId;
        this.eventsPerSec = eventsPerSec;
        this.secondsToRun = secondsToRun;
        this.StartTime = start;
        this.stats = stats;
        this.isRandomKey = isRandomKey;
        this.messageSize = messageSize;
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
        final long mSeconds = secondsToRun * 1000;
        long diffTime = mSeconds;

        do {

            long loopStartTime = System.currentTimeMillis();
            for (int i = 0; i < eventsPerSec; i++) {

                // Construct event payload
                String val = System.currentTimeMillis() + ", " + producerId + ", " + (int) (Math.random() * 200);
                String payload = String.format("%-" + messageSize + "s", val);
                String key;
                if (isRandomKey) {
                    key = Integer.toString(producerId + new Random().nextInt());
                } else {
                    key = Integer.toString(producerId);
                }

                long startTime = System.currentTimeMillis();
                retFuture = writeData(key, payload);
                // event ingestion
                retFuture =
                    retFuture = stats.recordTime(retFuture, startTime, payload.length());
            }

            long timeSpent = System.currentTimeMillis() - loopStartTime;
            if (timeSpent < 1000) {
                Thread.sleep(1000 - timeSpent);
            }

            diffTime = System.currentTimeMillis() - StartTime;
        } while (diffTime < mSeconds);

        producer.flush();
        // producer.close();

        //Wait for the last packet to get acked
        retFuture.get();

        return null;
    }
}