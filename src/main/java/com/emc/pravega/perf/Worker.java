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

/**
 *  abstract class for Writers and Readers.
 */
public abstract class Worker {
    final int workerID;
    final int eventsPerWorker;
    final int messageSize;
    final int timeout;
    final boolean isRandomKey;
    final Instant StartTime;
    final PerfStats stats;
    final int secondsToRun;

    Worker(int sensorId, int eventsPerWorker, int secondsToRun,
           boolean isRandomKey, int messageSize, Instant start,
           PerfStats stats, String streamName, int timeout) {
        this.workerID = sensorId;
        this.eventsPerWorker = eventsPerWorker;
        this.secondsToRun = secondsToRun;
        this.StartTime = start;
        this.stats = stats;
        this.isRandomKey = isRandomKey;
        this.messageSize = messageSize;
        this.timeout = timeout;
    }
}
