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
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PravegaTransactionWriterWorker extends PravegaWriterWorker {
    private final AtomicReference<Transaction<String>> transaction;
    private final int transactionsPerCommit;
    private final AtomicInteger eventCount;

    PravegaTransactionWriterWorker(int sensorId, int eventsPerSec,
                                   int secondsToRun, boolean isRandomKey,
                                   int messageSize, Instant start,
                                   ClientFactory factory, PerfStats stats,
                                   String streamName,
                                   long totalEvents,
                                   int transactionsPerCommit) {

        super(sensorId, eventsPerSec, secondsToRun, isRandomKey,
                messageSize, start, factory, stats, streamName, totalEvents);

        this.transactionsPerCommit = transactionsPerCommit;
        eventCount = new AtomicInteger(0);
        transaction = new AtomicReference<Transaction<String>>(producer.beginTxn());
    }

    @Override
    public CompletableFuture writeData(String key, String data) throws TxnFailedException, IllegalStateException {
        final Transaction<String> curTrans = transaction.get();

        curTrans.writeEvent(key, data);
        if (eventCount.incrementAndGet() >= transactionsPerCommit) {
            eventCount.set(0);
            curTrans.commit();
            if (!transaction.compareAndSet(curTrans, producer.beginTxn())) {
                throw new IllegalStateException("WriteData called on the same PravegaTransactionWriterWorker from two threads in parallel.");
            }
        }
        return null;
    }
}