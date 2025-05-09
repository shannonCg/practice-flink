package com.shaice.flink.redis;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.redisson.api.RedissonClient;

import java.io.IOException;

public class RedisSink implements Sink<String> {
    private transient RedissonClient redisson;

    @Override
    public SinkWriter<String> createWriter(InitContext initContext) throws IOException {
        return new RedisSinkWriter();
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new RedisSinkWriter();
    }
}
