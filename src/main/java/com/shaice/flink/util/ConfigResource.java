package com.shaice.flink.util;

import java.text.MessageFormat;

public class ConfigResource {
    private static final String configBasePath = "/config/";
    private static final String applicationFileName = "application.properties";
    private static String nodeFileNameTemplate = "node-{0}.properties";
    private static String kafkaFileNameTemplate = "kafka-{0}.properties";

    public static String getNodeFilePath(String environment) {
        return configBasePath+ MessageFormat.format(nodeFileNameTemplate, environment);
    }

    public static String getKafkaFilePath(String environment) {
        return configBasePath + MessageFormat.format(kafkaFileNameTemplate, environment);
    }

    public static String getApplicationFilePath() {
        return configBasePath + applicationFileName;
    }
}
