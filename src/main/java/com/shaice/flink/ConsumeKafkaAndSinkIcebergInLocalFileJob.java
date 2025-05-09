package com.shaice.flink;

import com.shaice.flink.config.KafkaConfig;
import com.shaice.flink.util.ParameterToolUtil;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConsumeKafkaAndSinkIcebergInLocalFileJob {
    private static final int checkpointIntervalSec = 10;

    public void startJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint setting(record consumer offset)
        env.enableCheckpointing(checkpointIntervalSec * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(300 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointIntervalSec * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ParameterToolUtil parameterToolUtil = new ParameterToolUtil();
        KafkaConfig kafkaConfig = parameterToolUtil.getKafkaConfig();

        // Kafka Source Table
        tableEnv.executeSql(
                "CREATE TABLE kafka_table (" +
                        "status BOOLEAN, " +
                        "msg STRING, " +
                        "createTime BIGINT" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = '"+kafkaConfig.getTestConsumerTopic()+"'," +
                        "'properties.bootstrap.servers' = '"+kafkaConfig.getBroker()+"'," +
                        "'properties.group.id' = '"+kafkaConfig.getTestConsumerGroupId()+"'," +
                        "'scan.startup.mode' = 'earliest-offset'," +
                        "'format' = 'json'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE iceberg_table (" +
                        "status BOOLEAN, " +
                        "msg STRING, " +
                        "createTime BIGINT" +
                        ") WITH (" +
                        "'connector' = 'iceberg'," +
                        "'catalog-type' = 'hadoop'," +
                        "'catalog-name' = 'local_catalog'," +
                        "'warehouse' = 'file:///Users/zhangyining/flink/data'," +
                        "'format-version' = '1'" +
                        ")"
        );

        tableEnv.executeSql("INSERT INTO iceberg_table "+
                "SELECT status, msg, createTime FROM kafka_table"
        );
    }
}
