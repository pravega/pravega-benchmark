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
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class PravegaTransactionWriterWorker extends WriterWorker {
    private static Logger log = LoggerFactory.getLogger(PravegaTransactionWriterWorker.class);

    private static TriConsumer noOpTriConsumer = (a, b, c) -> {};

    private final TransactionalEventStreamWriter<byte[]> producer;
    private final int transactionsPerCommit;
    private final boolean enableWatermark;

    @GuardedBy("this")
    private int eventCount;

    // If null, a transaction has not been started.
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
                                   boolean enableWatermark, AtomicLong[] seqNum, Boolean isEnableRoutingKey) {
        super(sensorId, events, Integer.MAX_VALUE,
                secondsToRun, isRandomKey, messageSize, start,
                stats, streamName, eventsPerSec, writeAndRead, seqNum, isEnableRoutingKey);

        final String writerId = UUID.randomUUID().toString();
        this.producer = factory.createTransactionalEventWriter(
                writerId,
                streamName,
                new ByteArraySerializer(),
                EventWriterConfig.builder()
                        .enableConnectionPooling(enableConnectionPooling)
                        .build());
        this.transactionsPerCommit = transactionsPerCommit;
        this.enableWatermark = enableWatermark;

        eventCount = 0;
    }

    /**
     * Writes an event in a transaction. It will begin a new transaction if needed.
     * It periodically commits the current transaction.
     * Called directly by write-only tests.
     *
     * @param data   data to write
     * @param record to call to record statistics
     * @return the current time
     */
    @Override
    public long recordWrite(byte[] data, TriConsumer record) {
        long time = 0;
        try {
            synchronized (this) {
                time = System.currentTimeMillis();
                if (transaction == null) {
                    transaction = producer.beginTxn();
                }
                log.info("Event write: {}", new String(data));
                transaction.writeEvent(data);
                record.accept(time, System.currentTimeMillis(), messageSize);
                eventCount++;
                if (eventCount >= transactionsPerCommit) {
                    eventCount = 0;
                    if (enableWatermark) {
                        log.debug("recordWrite: commit({})", time);
                        transaction.commit(time);
                    } else {
                        transaction.commit();
                    }
                    transaction = null;
                }
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException("Transaction Write data failed ", e);
        }
        return time;
    }

    /**
     * Used only by read-write tests.
     * @param data data to write
     */
    @Override
    public void writeData(byte[] data) {
        recordWrite(data, noOpTriConsumer);
    }

    /**
     * Commits the current transaction if it exists.
     */
    @Override
    public void flush() {
        try {
            synchronized (this) {
                if (transaction != null) {
                    if (enableWatermark) {
                        long time = System.currentTimeMillis();
                        log.debug("flush: commit({})", time);
                        transaction.commit(time);
                    } else {
                        transaction.commit();
                    }
                    transaction = null;
                }
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void close() {
        if (transaction != null) {
            transaction.abort();
            transaction = null;
        }
        producer.close();
    }
}
