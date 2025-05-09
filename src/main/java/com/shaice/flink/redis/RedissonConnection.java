package com.shaice.flink.redis;

import com.shaice.flink.config.RedisConfig;
import com.shaice.flink.util.ParameterToolUtil;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import java.io.IOException;

public class RedissonConnection {
    public static RedissonClient getConnection() {
        ParameterToolUtil parameterToolUtil = new ParameterToolUtil();
        RedisConfig redisConfig = null;
        try {
            redisConfig = parameterToolUtil.getRedisConfig();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Redisson.create(redisConfig.getRedissonConfig());
    }

}
