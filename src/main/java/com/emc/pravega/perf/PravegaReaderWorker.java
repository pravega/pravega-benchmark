/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.perf;

import java.time.Instant;
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
                        Instant start, PerfStats stats, String readergrp,
                        int timeout, ClientFactory factory) {
        super(readerId, events, secondsToRun, start, stats, readergrp, timeout);

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
