/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.perf;

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

public class PravegaStreamHandler {
    final String scope;
    final String stream;
    final String controllerUri;
    final ControllerImpl controller;
    final StreamManager streamManager;
    final StreamConfiguration streamconfig;
    ReaderGroupManager readerGroupManager;
    ReaderGroup readerGroup;
    final ScheduledExecutorService bgexecutor;
    final int segCount;
    final int timeout;

    PravegaStreamHandler(String scope, String stream,
                         String uri, int segs,
                         int timeout, ControllerImpl contrl,
                         ScheduledExecutorService bgexecutor) throws Exception {
        this.scope = scope;
        this.stream = stream;
        this.controllerUri = uri;
        this.controller = contrl;
        this.segCount = segs;
        this.timeout = timeout;
        this.readerGroup = null;
        this.bgexecutor = bgexecutor;
        streamManager = StreamManager.create(new URI(uri));
        streamManager.createScope(scope);
        streamconfig = StreamConfiguration.builder().scope(scope).streamName(stream)
                                          .scalingPolicy(ScalingPolicy.fixed(segCount))
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

    ReaderGroup createReaderGroup() throws URISyntaxException {
        if (readerGroup != null) {
            return readerGroup;
        }

        readerGroupManager = ReaderGroupManager.withScope(scope,
            ClientConfig.builder()
                        .controllerURI(new URI(controllerUri)).build());
        readerGroupManager.createReaderGroup(stream,
            ReaderGroupConfig.builder()
                             .stream(Stream.of(scope, stream))
                             .build());
        readerGroup = readerGroupManager.getReaderGroup(stream);
        return readerGroup;
    }
}
