package com.shaice.flink.redis;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RedisSinkWriter implements SinkWriter<String> {
    private final RedissonClient redissonClient;

    public RedisSinkWriter() {
        this.redissonClient = RedissonConnection.getConnection();
    }

    @Override
    public void write(String o, Context context) throws IOException, InterruptedException {
        String[] parts = o.split(":");
        if(parts.length == 2){
            String key = parts[0];
            String value = parts[1];

//            RBatch batch = redissonClient.createBatch();
//            RMapAsync<String, Object> testMap = batch.getMap("flink_test");
//            testMap.putAsync(key, value);
//            testMap.expireAsync(1, TimeUnit.HOURS);
//            batch.execute();

            RMap<String, Object> map = redissonClient.getMap("flink_test");
            map.put(key, value);
            map.expire(1, TimeUnit.HOURS);
        } else {
            throw new IllegalArgumentException("Invalid input format: " + o);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {
        if (redissonClient != null && redissonClient.isShutdown() == false) {
            redissonClient.shutdown();
        }
    }
}
