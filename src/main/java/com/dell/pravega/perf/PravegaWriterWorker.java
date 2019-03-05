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
                        PerfStats stats, String streamName, int eventsPerSec, ClientFactory factory) {

        super(sensorId, events, secondsToRun,
                isRandomKey, messageSize, start,
                stats, streamName, eventsPerSec);

        this.producer = factory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
    }


    @Override
    public void recordWrite(String data, TriConsumer record) {
        CompletableFuture ret;
        final long startTime = System.currentTimeMillis();
        ret = producer.writeEvent(data);
        if (ret == null) {
            record.accept(startTime, System.currentTimeMillis(), messageSize);
        } else {
            ret.thenAccept(d -> {
                record.accept(startTime, System.currentTimeMillis(), messageSize);
            });
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }
}