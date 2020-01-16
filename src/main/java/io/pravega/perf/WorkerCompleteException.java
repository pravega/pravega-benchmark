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
        return "Worker "+workerId+" Complete";
    }
}
