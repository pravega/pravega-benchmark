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

import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;

/**
 * class for Pravega reader/consumer.
 */
public class PravegaReaderWorker extends ReaderWorker {
    private final EventStreamReader<String> reader;
    private final String readerId;

    PravegaReaderWorker(int readerId, int events, int secondsToRun,
                        long start, PerfStats stats, String readergrp,
                        int timeout, boolean wNr, ClientFactory factory) {
        super(readerId, events, secondsToRun, start, stats, readergrp, timeout, wNr);

        this.readerId = Integer.toString(readerId);
        reader = factory.createReader(
                this.readerId, readergrp, new UTF8StringSerializer(), ReaderConfig.builder().build());
    }

    @Override
    public String readData() {
        try {
            String data = reader.readNextEvent(timeout).getEvent();
            return data;
        } catch (ReinitializationRequiredException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        reader.close();
    }
}
