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

/**
 *  abstract class for Writers and Readers.
 */
public abstract class Worker {
    final long totalEvents;
    final int workerID;
    final int eventsPerSec;
    final int secondsToRun;
    final int messageSize;
    final Instant StartTime;
    final boolean isRandomKey;
    final PerfStats stats;
    final int timeout;

    Worker(int sensorId, int eventsPerSec, int secondsToRun,
           boolean isRandomKey, int messageSize, Instant start,
           PerfStats stats, String streamName, long totalEvents, int timeout) {
        this.workerID = sensorId;
        this.eventsPerSec = eventsPerSec;
        this.secondsToRun = secondsToRun;
        this.StartTime = start;
        this.stats = stats;
        this.isRandomKey = isRandomKey;
        this.messageSize = messageSize;
        this.totalEvents = totalEvents;
        this.timeout = timeout;
    }
}
