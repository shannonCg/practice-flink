package com.shaice.flink;

import com.shaice.flink.config.KafkaConfig;
import com.shaice.flink.redis.RedisSink;
import com.shaice.flink.util.ParameterToolUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class ConsumeKafkaAndSinkRedisJob {
    private static final int checkpointIntervalSec = 1;

    public void startJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint setting(record consumer offset)
        env.enableCheckpointing(checkpointIntervalSec * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(300 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointIntervalSec * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoint");
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        env.configure(configuration);

        ParameterToolUtil parameterToolUtil = new ParameterToolUtil();
        KafkaConfig kafkaConfig = parameterToolUtil.getKafkaConfig();
        KafkaSource<String> kafkaConsumer = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaConfig.getBroker())
                .setTopics(kafkaConfig.getTestConsumerTopic())
                .setGroupId(kafkaConfig.getTestConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) //consume from the earliest offset if no offset record store in checkpoint, else consume from the recorded offset
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "5000") //for detect new partitions, default for 10 min
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed") //read committed message for transaction commit message send by producer
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("Kafka Source")
                .name("Kafka Source");

        sourceStream.sinkTo(new RedisSink());

        env.execute("Consume Kafka and Sink to Redis");
    }
}
