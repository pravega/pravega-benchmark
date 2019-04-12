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

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Class for Kafka writer/producer.
 */
public class KafkaWriterWorker extends WriterWorker {
    final private KafkaProducer<String, String> producer;

    KafkaWriterWorker(int sensorId, int events, int secondsToRun,
                      boolean isRandomKey, int messageSize, long start,
                      PerfStats stats, String streamName, int eventsPerSec,
                      boolean writeAndRead, Properties producerProps) {

        super(sensorId, events, secondsToRun,
                isRandomKey, messageSize, start,
                stats, streamName, eventsPerSec, writeAndRead);

        this.producer = new KafkaProducer<>(producerProps);
    }

    public long recordWrite(String data, TriConsumer record) {
        final long time = System.currentTimeMillis();
        producer.send(new ProducerRecord<>(streamName, data), (metadata, exception) -> {
            record.accept(time, System.currentTimeMillis(), data.length());
        });
        return time;
    }

    @Override
    public void writeData(String data) {
        producer.send(new ProducerRecord<>(streamName, data));
        /*
          flush is required here for following reasons:
          1. The writeData is called for End to End latency mode; hence make sure that data is sent.
          2. kafka buffering makes the too many writes; flushing will moderate the kafka producer.
          3. If the flush called after several iterations, then flush will take too much of time.
         */
        flush();
    }


    @Override
    public void flush() {
        producer.flush();
    }
}