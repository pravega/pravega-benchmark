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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for Pravega stream and segments.
 */
public class PravegaStreamHandler {
    private static Logger log = LoggerFactory.getLogger(PravegaStreamHandler.class);

    final String scope;
    final String stream;
    final String rdGrpName;
    final String controllerUri;
    final ControllerImpl controller;
    final StreamManager streamManager;
    final ScheduledExecutorService bgexecutor;
    final int segCount;
    final int timeout;
    final int segmentScaleKBps;
    final int segmentScaleEventsPerSecond;
    final int scaleFactor;

    ReaderGroupManager readerGroupManager;
    ReaderGroupConfig rdGrpConfig;

    PravegaStreamHandler(String scope, String stream, String rdGrpName, String uri, int segments, int segmentScaleKBps,
                         int segmentScaleEventsPerSecond, int scaleFactor, int timeout, ControllerImpl controller,
                         ScheduledExecutorService bgexecutor, boolean createScope) throws Exception {
        this.scope = scope;
        this.stream = stream;
        this.rdGrpName = rdGrpName;
        this.controllerUri = uri;
        this.controller = controller;
        this.segCount = segments;
        this.timeout = timeout;
        this.bgexecutor = bgexecutor;
        this.scaleFactor = scaleFactor;
        this.segmentScaleKBps = segmentScaleKBps;
        this.segmentScaleEventsPerSecond = segmentScaleEventsPerSecond;
        this.streamManager = StreamManager.create(new URI(uri));

        if (createScope) {
            this.streamManager.createScope(scope);
        }
    }

    boolean create() {
        return streamManager.createStream(scope, stream, getStreamConfig());
    }

    void scale() throws InterruptedException, ExecutionException, TimeoutException {
        StreamSegments segments = controller.getCurrentSegments(scope, stream).join();
        final int nseg = segments.getSegments().size();
        log.info("Current segments of the stream: {} = {}", stream, nseg);

        if (nseg == segCount || segCount == -1) {
            log.info("Not modifying existing stream");
            return;
        }

        log.info("The stream: {} will be manually scaling to {} segments", stream, segCount);

        /*
         * Note that the Upgrade stream API does not change the number of segments;
         * but it indicates with new number of segments.
         * after calling update stream , manual scaling is required
         */
        if (!streamManager.updateStream(scope, stream, getStreamConfig())) {
            throw new TimeoutException("Could not able to update the stream: " + stream + " try with another stream Name");
        }

        final double keyRangeChunk = 1.0 / segCount;
        final Map<Double, Double> keyRanges = IntStream.range(0, segCount)
                .boxed()
                .collect(
                        Collectors
                                .toMap(x -> x * keyRangeChunk,
                                        x -> (x + 1) * keyRangeChunk));
        final List<Long> segmentList = segments.getSegments()
                .stream()
                .map(Segment::getSegmentId)
                .collect(Collectors.toList());

        CompletableFuture<Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope, stream),
                segmentList,
                keyRanges,
                bgexecutor).getFuture();

        if (!scaleStatus.get(timeout, TimeUnit.SECONDS)) {
            throw new TimeoutException("ERROR : Scale operation on stream " + stream + " did not complete");
        }

        System.out.println("Number of Segments after manual scale: " +
                controller.getCurrentSegments(scope, stream)
                        .get().getSegments().size());
    }

    void recreate() throws InterruptedException, ExecutionException, TimeoutException {
        log.info("Sealing and Deleting the stream : {} and then recreating the same", stream);
        CompletableFuture<Boolean> sealStatus = controller.sealStream(scope, stream);
        if (!sealStatus.get(timeout, TimeUnit.SECONDS)) {
            throw new TimeoutException("ERROR : Segment sealing operation on stream " + stream + " did not complete");
        }

        CompletableFuture<Boolean> status = controller.deleteStream(scope, stream);
        if (!status.get(timeout, TimeUnit.SECONDS)) {
            throw new TimeoutException("ERROR : stream: " + stream + " delete failed");
        }

        if (!streamManager.createStream(scope, stream, getStreamConfig())) {
            throw new TimeoutException("ERROR : stream: " + stream + " recreation failed");
        }
    }

    ReaderGroup createReaderGroup(boolean reset, ClientConfig clientConfig) throws URISyntaxException {
        if (readerGroupManager == null) {
            readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
            rdGrpConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, stream)).build();
        }
        readerGroupManager.createReaderGroup(rdGrpName, rdGrpConfig);
        final ReaderGroup rdGroup = readerGroupManager.getReaderGroup(rdGrpName);
        if (reset) {
            rdGroup.resetReaderGroup(rdGrpConfig);
        }
        return rdGroup;
    }

    void deleteReaderGroup() {
        try {
            readerGroupManager.deleteReaderGroup(rdGrpName);
        } catch (RuntimeException e) {
            log.info("Cannot delete reader group {} because it is already deleted", rdGrpName);
        }
    }

    private StreamConfiguration getStreamConfig() {
        ScalingPolicy scalingPolicy = null;
        if (segmentScaleEventsPerSecond == 0 && segmentScaleKBps == 0) {
            scalingPolicy = ScalingPolicy.fixed(segCount);
        } else if (segmentScaleKBps > 0) {
            scalingPolicy = ScalingPolicy.byDataRate(segmentScaleKBps, scaleFactor, segCount);
        } else if (segmentScaleEventsPerSecond > 0) {
            scalingPolicy = ScalingPolicy.byEventRate(segmentScaleEventsPerSecond, scaleFactor, segCount);
        }

        return StreamConfiguration.builder()
            .scalingPolicy(scalingPolicy)
            .build();
    }
}
