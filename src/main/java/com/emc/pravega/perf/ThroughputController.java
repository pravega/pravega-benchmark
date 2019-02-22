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

import java.time.Duration;
import java.time.Instant;

public class ThroughputController {
    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    private final long sleepTimeNs;
    private final long eventsPerSec;
    private long timeNs = 0;

    /**
     * @param eventsPerSec events per second
     */
    public ThroughputController(int eventsPerSec) {
        this.eventsPerSec = eventsPerSec;
        this.sleepTimeNs = this.eventsPerSec > 0 ?
                NS_PER_SEC / this.eventsPerSec : 0;
    }

    /**
     * @param messageSize      message size in bytes
     * @param targetThroughput target throughput in MB/s
     */
    public ThroughputController(long messageSize, double targetThroughput) {
        this((int) ((targetThroughput * 1024 * 1024) / messageSize));
    }

    /**
     * blocks for small amounts of time to achieve targetThroughput/events per sec
     *
     * @param eventsRate number of events/sec till now
     */
    public synchronized void control(int eventsRate) {

        if ((this.eventsPerSec > 0) && (eventsRate > this.eventsPerSec)) {
            // control throughput / number of events by sleeping, on average,
            timeNs += sleepTimeNs;

            // If threshold reached, sleep a little
            if (timeNs >= MIN_SLEEP_NS) {
                Instant sleepStart = Instant.now();
                try {
                    final long sleepMs = timeNs / NS_PER_MS;
                    final long sleepNs = timeNs - sleepMs * NS_PER_MS;
                    Thread.sleep(sleepMs, (int) sleepNs);
                } catch (InterruptedException e) {
                    // will be taken care in finally block
                } finally {
                    // in case of short sleeps or oversleep ;adjust it for next sleep duration
                    final long sleptNS = Duration.between(sleepStart, Instant.now()).toNanos();
                    if (sleptNS > 0) {
                        timeNs -= sleptNS;
                    } else {
                        timeNs = 0;
                    }
                }
            }
        }
    }
}
