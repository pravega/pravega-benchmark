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

public class EventsController {
    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;
    private final long startTime;
    private final long sleepTimeNs;
    private final int eventsPerSec;
    private long timeNs = 0;

    /**
     * @param eventsPerSec events per second
     */
    public EventsController(long start, int eventsPerSec) {
        this.startTime = start;
        this.eventsPerSec = eventsPerSec;
        this.sleepTimeNs = this.eventsPerSec > 0 ?
                NS_PER_SEC / this.eventsPerSec : 0;
    }

    /**
     * blocks for small amounts of time to achieve targetThroughput/events per sec
     *
     * @param events current events
     */
    public void control(long events) {
        if (this.eventsPerSec <= 0) {
            return;
        }

        float elapsedSec = (System.currentTimeMillis() - startTime) / 1000.f;

        if ((events / elapsedSec) < this.eventsPerSec) {
            return;
        }

        // control throughput / number of events by sleeping, on average,
        timeNs += sleepTimeNs;
        // If threshold reached, sleep a little
        if (timeNs >= MIN_SLEEP_NS) {
            long sleepStart = System.nanoTime();
            try {
                final long sleepMs = timeNs / NS_PER_MS;
                final long sleepNs = timeNs - sleepMs * NS_PER_MS;
                Thread.sleep(sleepMs, (int) sleepNs);
            } catch (InterruptedException e) {
                // will be taken care in finally block
            } finally {
                // in case of short sleeps or oversleep ;adjust it for next sleep duration
                final long sleptNS = System.nanoTime() - sleepStart;
                if (sleptNS > 0) {
                    timeNs -= sleptNS;
                } else {
                    timeNs = 0;
                }
            }
        }
    }
}