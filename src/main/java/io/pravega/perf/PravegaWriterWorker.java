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

import java.util.concurrent.CompletableFuture;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.TxnFailedException;

/**
 * class for Pravega writer/producer.
 */
public class PravegaWriterWorker extends WriterWorker {
    final EventStreamWriter<String> producer;

    PravegaWriterWorker(int sensorId, int events, int secondsToRun,
                        boolean isRandomKey, int messageSize, long start,
                        PerfStats stats, String streamName, int eventsPerSec,
                        boolean wNr, ClientFactory factory) {

        super(sensorId, events, secondsToRun,
                isRandomKey, messageSize, start,
                stats, streamName, eventsPerSec, wNr);

        this.producer = factory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
    }


    @Override
    public long recordWrite(String data, TriConsumer record) {
        CompletableFuture ret;
        final long time = System.currentTimeMillis();
        ret = producer.writeEvent(data);
        ret.thenAccept(d -> {
            record.accept(time, System.currentTimeMillis(), data.length());
        });
        return time;
    }

    @Override
    public void writeData(String data) {
        producer.writeEvent(data);
    }

    @Override
    public void flush() {
        producer.flush();
    }
}