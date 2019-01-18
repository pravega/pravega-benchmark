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

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.TxnFailedException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * class for Pravega writer/producer.
 */
public class PravegaWriterWorker extends WriterWorker {
    final EventStreamWriter<String> producer;

    PravegaWriterWorker(int sensorId, int eventsPerWorker, int secondsToRun,
                        boolean isRandomKey, int messageSize, Instant start,
                        PerfStats stats, String streamName, ClientFactory factory) {

        super(sensorId, eventsPerWorker, secondsToRun,
                isRandomKey, messageSize, start,
                stats, streamName);

        this.producer = factory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
    }

    @Override
    public CompletableFuture writeData(String key, String data) {
        return producer.writeEvent(key, data);
    }

    @Override
    public void flush() {
        producer.flush();
    }
}