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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

public class PravegaTransactionWriterWorker extends PravegaWriterWorker {
    private static Logger log = LoggerFactory.getLogger(PravegaTransactionWriterWorker.class);

    private final int transactionsPerCommit;
    private final boolean enableWatermark;

    @GuardedBy("this")
    private int eventCount;

    @GuardedBy("this")
    private Transaction<byte[]> transaction;

    /**
     *
     * @param enableWatermark If true, the current time will be provided each time the transaction is committed.
     */
    PravegaTransactionWriterWorker(int sensorId, int events,
                                   int secondsToRun, boolean isRandomKey,
                                   int messageSize, long start,
                                   PerfStats stats, String streamName, int eventsPerSec, boolean writeAndRead,
                                   EventStreamClientFactory factory, int transactionsPerCommit, boolean enableConnectionPooling,
                                   boolean enableWatermark) {
        super(sensorId, events, Integer.MAX_VALUE, secondsToRun, isRandomKey,
                messageSize, start, stats, streamName, eventsPerSec, writeAndRead, factory, enableConnectionPooling,
                -1);

        this.transactionsPerCommit = transactionsPerCommit;
        this.enableWatermark = enableWatermark;
        eventCount = 0;
        transaction = producer.beginTxn();
    }

    @Override
    public long recordWrite(byte[] data, TriConsumer record) {
        long time = 0;
        try {
            synchronized (this) {
                time = System.currentTimeMillis();
                transaction.writeEvent(data);
                record.accept(time, System.currentTimeMillis(), messageSize);
                eventCount++;
                if (eventCount >= transactionsPerCommit) {
                    eventCount = 0;
                    if (enableWatermark) {
                        log.info("recordWrite: commit({})", time);
                        transaction.commit(time);
                    } else {
                        transaction.commit();
                    }
                    transaction = producer.beginTxn();
                }
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException("Transaction Write data failed ", e);
        }
        return time;
    }
}