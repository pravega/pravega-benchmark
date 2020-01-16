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
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class for Pravega reader/consumer.
 */
public class PravegaStreamingReaderWorker extends ReaderWorker {
    private static Logger log = LoggerFactory.getLogger(PravegaStreamingReaderWorker.class);

    private final EventStreamReader<byte[]> reader;
    private final Stream stream;
    private final ScheduledExecutorService watermarkExecutor = Executors.newScheduledThreadPool(1);

    /**
     *
     * @param readWatermarkPeriodMillis If >0, watermarks will be read with a period of this many milliseconds.
     */
    PravegaStreamingReaderWorker(int readerId, int events, int secondsToRun,
                                 long start, PerfStats stats, String readergrp,
                                 int timeout, boolean writeAndRead, EventStreamClientFactory factory,
                                 Stream stream, long readWatermarkPeriodMillis) {
        super(readerId, events, secondsToRun, start, stats, readergrp, timeout, writeAndRead);

        final String readerSt = Integer.toString(readerId);
        reader = factory.createReader(
                readerSt, readergrp, new ByteArraySerializer(), ReaderConfig.builder().build());
        this.stream = stream;

        if (readWatermarkPeriodMillis > 0) {
            watermarkExecutor.scheduleAtFixedRate(this::readWatermark, readWatermarkPeriodMillis, readWatermarkPeriodMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public byte[] readData() {
        try {
            return reader.readNextEvent(timeout).getEvent();
        } catch (ReinitializationRequiredException e) {
            throw new IllegalStateException(e);
        }
    }

    private void readWatermark() {
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(stream);
        log.debug("readWatermark: currentTimeWindow={}", currentTimeWindow);
    }

    @Override
    public void close() {
        watermarkExecutor.shutdown();
        try {
            watermarkExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        reader.close();
    }
}
