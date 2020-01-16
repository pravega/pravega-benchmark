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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import com.google.common.collect.Streams;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ControllerImpl;
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

/**
 * Class for Pravega stream and segments.
 */
public class PravegaStreamHandler {
    final String scope;
    final String stream;
    final String rdGrpName;
    final String controllerUri;
    final ControllerImpl controller;
    final StreamManager streamManager;
    final StreamConfiguration streamconfig;
    final ScheduledExecutorService bgexecutor;
    final int segCount;
    final int timeout;
    ReaderGroupManager readerGroupManager;
    ReaderGroupConfig rdGrpConfig;

    PravegaStreamHandler(String scope, String stream, String rdGrpName, String uri, int segments, int segmentScaleKBps,
                         int segmentScaleEventsPerSecond, int scaleFactor, int timeout, ControllerImpl controller,
                         ScheduledExecutorService bgexecutor) throws Exception {
        this.scope = scope;
        this.stream = stream;
        this.rdGrpName = rdGrpName;
        this.controllerUri = uri;
        this.controller = controller;
        this.segCount = segments;
        this.timeout = timeout;
        this.bgexecutor = bgexecutor;
        this.streamManager = StreamManager.create(new URI(uri));
        this.streamManager.createScope(scope);

        ScalingPolicy scalingPolicy = null;
        if (segmentScaleEventsPerSecond == 0 && segmentScaleKBps == 0) {
            scalingPolicy = ScalingPolicy.fixed(segCount);
        } else if (segmentScaleKBps > 0) {
            scalingPolicy = ScalingPolicy.byDataRate(segmentScaleKBps, scaleFactor, segCount);
        } else if (segmentScaleEventsPerSecond > 0) {
            scalingPolicy = ScalingPolicy.byEventRate(segmentScaleEventsPerSecond, scaleFactor, segCount);
        }

        this.streamconfig = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();
    }

    boolean create() {
        return streamManager.createStream(scope, stream, streamconfig);
    }

    void scale() throws InterruptedException, ExecutionException, TimeoutException {
        StreamSegments segments = controller.getCurrentSegments(scope, stream).join();
        final int nseg = segments.getSegments().size();
        System.out.println("Current segments of the stream: " + stream + " = " + nseg);

        if (nseg == segCount) {
            return;
        }

        System.out.println("The stream: " + stream + " will be manually scaling to " + segCount + " segments");

        /*
         * Note that the Upgrade stream API does not change the number of segments;
         * but it indicates with new number of segments.
         * after calling update stream , manual scaling is required
         */
        if (!streamManager.updateStream(scope, stream, streamconfig)) {
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
        System.out.println("Sealing and Deleteing the stream : " + stream + " and then recreating the same");
        CompletableFuture<Boolean> sealStatus = controller.sealStream(scope, stream);
        if (!sealStatus.get(timeout, TimeUnit.SECONDS)) {
            throw new TimeoutException("ERROR : Segment sealing operation on stream " + stream + " did not complete");
        }

        CompletableFuture<Boolean> status = controller.deleteStream(scope, stream);
        if (!status.get(timeout, TimeUnit.SECONDS)) {
            throw new TimeoutException("ERROR : stream: " + stream + " delete failed");
        }

        if (!streamManager.createStream(scope, stream, streamconfig)) {
            throw new TimeoutException("ERROR : stream: " + stream + " recreation failed");
        }
    }



    ReaderGroup createReaderGroup(boolean reset) throws URISyntaxException {
        if (readerGroupManager == null) {
            readerGroupManager = ReaderGroupManager.withScope(scope,
                    ClientConfig.builder().controllerURI(new URI(controllerUri)).build());
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

    public BatchClientFactory newBatchClientFactory() {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerUri)).build();
        return BatchClientFactory.withScope(scope, clientConfig);
    }

    public List<SegmentRange> getBatchSegmentRanges(BatchClientFactory batchFactory) {
        Iterator<SegmentRange> segmentRangeIterator = batchFactory.getSegments(Stream.of(scope, stream), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED)
            .getIterator();

        return Streams.stream(segmentRangeIterator).collect(Collectors.toList());
    }

    void deleteReaderGroup() {
        try {
            readerGroupManager.deleteReaderGroup(rdGrpName);
        } catch (RuntimeException e) {
            System.out.println("Cannot delete reader group " + rdGrpName + " because it is already deleted");
        }
    }
}
