package io.pravega.perf;

import io.pravega.client.BatchClientFactory;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class PravegaBatchReaderWorker extends ReaderWorker {
    private static Logger log = LoggerFactory.getLogger(PravegaBatchReaderWorker.class);

    private final BatchClientFactory batchClientFactory;
    private final Iterator<SegmentRange> assignedSegments;

    private SegmentIterator<byte[]> currentSegmentIterator;
    private SegmentRange currentRange;
    private boolean finished;

    PravegaBatchReaderWorker(int readerId, int events, int secondsToRun, long start, PerfStats stats, String readerGrp, int timeout, boolean writeAndRead, BatchClientFactory batchClientFactory, List<SegmentRange> assignedSegments) {
        super(readerId, events, secondsToRun, start, stats, readerGrp, timeout, writeAndRead);
        this.batchClientFactory = batchClientFactory;

        this.assignedSegments = assignedSegments.iterator();
    }

    @Override
    public byte[] readData() {
        if (finished) {
            return null;
        }

        if (currentSegmentIterator == null || !currentSegmentIterator.hasNext()) {
            if (currentRange != null) {
                currentSegmentIterator.close();

                log.info("id:{} Completed Segment {}, {}({}:{})",workerID, currentRange.getStreamName(), currentRange.getSegmentId(), currentRange.getStartOffset(), currentRange.getEndOffset());
            }

            if (assignedSegments.hasNext()) {

                currentRange = assignedSegments.next();
                currentSegmentIterator = batchClientFactory.readSegment(currentRange, new ByteArraySerializer());

                log.info("id:{} Starting Segment {}, {}({}:{})",workerID, currentRange.getStreamName(), currentRange.getSegmentId(), currentRange.getStartOffset(), currentRange.getEndOffset());
            } else {
                log.info("id:{} Completed all assigned assignedSegments", workerID);
                currentSegmentIterator = null;
                finished = true;
                return null;
            }
        }

        return currentSegmentIterator.next();
    }

    @Override
    public void close() {
        if (currentSegmentIterator != null) {
            currentSegmentIterator.close();
        }
    }
}
