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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;


/**
 *  class for Pravega reader/consumer.
 */
public class PravegaReaderWorker extends ReaderWorker  {
    private final static AtomicInteger eventCount = new AtomicInteger(0);

    private final EventStreamReader<String> reader;
    private final String readerId;

    PravegaReaderWorker(int readerId, int secondsToRun, Instant start,
                        PerfStats stats, String readergrp, long totalEvents,
                        int timeout, ClientFactory factory) {
        super(readerId, secondsToRun, start, stats, readergrp, totalEvents, timeout);

        this.readerId = Integer.toString(readerId);
        reader = factory.createReader(
                this.readerId, readergrp, new UTF8StringSerializer(), ReaderConfig.builder().build());
    }

    @Override
    public  String readData() throws Exception {
        try {
            return reader.readNextEvent(timeout).getEvent();
        } catch(ReinitializationRequiredException e){
            throw new Exception("ReinitializationRequiredException exception while reading data ");
        }
    }

    @Override
    public void close(){
        reader.close();
    }

    @Override
    public long eventCountIncrementAndGet() {
        return eventCount.incrementAndGet();
    }

    @Override
    public long eventCountGet() {
        return eventCount.get();
    }

}
