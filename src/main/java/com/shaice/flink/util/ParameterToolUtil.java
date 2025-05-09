package com.shaice.flink.util;

import com.shaice.flink.config.KafkaConfig;
import com.shaice.flink.config.RedisConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

public class ParameterToolUtil {
    private static String env;

    public String getApplicationEnv() throws IOException {
        InputStream inStream = getClass().getResourceAsStream(ConfigResource.getApplicationFilePath());
        ParameterTool paramTool = ParameterTool.fromPropertiesFile(inStream);

        if(env == null) {
            env = paramTool.get("properties.postfix");
        }
        return env;
    }

    public KafkaConfig getKafkaConfig() throws IOException {
        InputStream inStream = getClass().getResourceAsStream(ConfigResource.getKafkaFilePath(getApplicationEnv()));
        ParameterTool paramTool = ParameterTool.fromPropertiesFile(inStream);

        KafkaConfig config = new KafkaConfig();
        config.setBroker(paramTool.get("broker"));
        config.setTestConsumerTopic(paramTool.get("test1.consumer.topic"));
        config.setTestConsumerGroupId(paramTool.get("test1.consumer.group"));
        config.setTestProducerTopic(paramTool.get("test1.producer.topic"));

        return config;

    }

    public RedisConfig getRedisConfig() throws IOException {
        InputStream inStream = getClass().getResourceAsStream(ConfigResource.getRedisFilePath(getApplicationEnv()));
        ParameterTool paramTool = ParameterTool.fromPropertiesFile(inStream);

        RedisConfig config = new RedisConfig();
        config.setRedisMasterAddr(paramTool.get("redis.masterAddr"));
        config.setRedisSlaveAddr(paramTool.get("redis.slaveAddr"));
        config.setRedisUserName(paramTool.get("redis.userName"));
        config.setRedisPassword(paramTool.get("redis.password"));
        config.setRedisRetryAttempts(paramTool.get("redis.retryAttempts"));
        config.setRedisConnectTimeout(paramTool.get("redis.connectTimeout"));

        return config;
    }
}
