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

/**
 * Signals that a worker has completed all it's assigned work
 */
public class WorkerCompleteException extends Exception {
    private final int workerId;

    public WorkerCompleteException(int workerId) {
        this.workerId = workerId;
    }

    public int getWorkerId() {
        return workerId;
    }

    @Override
    public String toString() {
        return "Worker " + workerId + " Complete";
    }
}
