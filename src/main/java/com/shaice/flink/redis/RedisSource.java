package com.shaice.flink.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.util.Map;

public class RedisSource extends RichSourceFunction<String> {
    private transient RedissonClient redissonClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.redissonClient = RedissonConnection.getConnection();
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        RMap<String, Object> testMap = redissonClient.getMap("flink_test");
        for(Map.Entry<String, Object> test : testMap.entrySet()) {
            sourceContext.collect(test.getKey() + ":" + test.getValue());
        }
    }

    @Override
    public void cancel() {
        if (redissonClient != null && redissonClient.isShutdown() == false) {
            redissonClient.shutdown();
        }
    }
}
