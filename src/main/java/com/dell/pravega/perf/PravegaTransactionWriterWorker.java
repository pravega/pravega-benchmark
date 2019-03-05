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
                                   int messageSize, long start,
                                   PerfStats stats, String streamName, int eventsPerSec,
                                   ClientFactory factory, int transactionsPerCommit) {

        super(sensorId, events, secondsToRun, isRandomKey,
                messageSize, start, stats, streamName, eventsPerSec, factory);

        this.transactionsPerCommit = transactionsPerCommit;
        eventCount = 0;
        transaction = producer.beginTxn();
    }

    @Override
    public void recordWrite(String key, String data, TriConsumer record) {
        try {
            synchronized (this) {
                final long startTime = System.currentTimeMillis();
                transaction.writeEvent(key, data);
                record.accept(startTime, System.currentTimeMillis(), messageSize);
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
    }
}