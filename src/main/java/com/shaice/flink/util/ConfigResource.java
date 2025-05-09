package com.shaice.flink.util;

import java.text.MessageFormat;

public class ConfigResource {
    private static final String CONFIG_BASE_PATH = "/config/";
    private static final String APPLICATION_FILE_NAME = "application.properties";
    private static final String KAFKA_FILE_NAME_TEMPLATE = "kafka-{0}.properties";
    private static final String REDIS_FILE_NAME_TEMPLATE = "redis-{0}.properties";

    public static String getApplicationFilePath() {
        return CONFIG_BASE_PATH + APPLICATION_FILE_NAME;
    }

    public static String getKafkaFilePath(String environment) {
        return CONFIG_BASE_PATH + MessageFormat.format(KAFKA_FILE_NAME_TEMPLATE, environment);
    }

    public static String getRedisFilePath(String environment) {
        return CONFIG_BASE_PATH + MessageFormat.format(REDIS_FILE_NAME_TEMPLATE, environment);
    }
}
