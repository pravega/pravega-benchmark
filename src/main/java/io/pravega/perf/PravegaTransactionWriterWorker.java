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

package io.pravega.perf;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;

public class PravegaTransactionWriterWorker extends PravegaWriterWorker {
    private final int transactionsPerCommit;

    @GuardedBy("this")
    private int eventCount;

    @GuardedBy("this")
    private Transaction<String> transaction;

    PravegaTransactionWriterWorker(int sensorId, int events,
                                   int secondsToRun, boolean isRandomKey,
                                   int messageSize, Instant start,
                                   PerfStats stats, String streamName, ThroughputController tput,
                                   ClientFactory factory, int transactionsPerCommit) {

        super(sensorId, events, secondsToRun, isRandomKey,
                messageSize, start, stats, streamName, tput, factory);

        this.transactionsPerCommit = transactionsPerCommit;
        eventCount = 0;
        transaction = producer.beginTxn();
    }

    @Override
    public CompletableFuture writeData(String key, String data) {
        try {
            synchronized (this) {
                transaction.writeEvent(key, data);
                eventCount++;
                if (eventCount >= transactionsPerCommit) {
                    eventCount = 0;
                    transaction.commit();
                    transaction = producer.beginTxn();
                }
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException("Transaction Write data failed ", e);
        }
        return null;
    }
}