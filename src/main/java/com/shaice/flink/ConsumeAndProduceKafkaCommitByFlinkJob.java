package com.shaice.flink;

import com.shaice.flink.config.KafkaConfig;
import com.shaice.flink.util.ParameterToolUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ConsumeAndProduceKafkaCommitByFlinkJob {
    private static final int checkpointIntervalSec = 10;

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
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) //consume from the earliest offset if no offset record store in checkpoint, else consume from the recorded offset
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "5000") //for detect new partitions, default for 10 min
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed") //read committed message for transaction commit message send by producer
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("Kafka Source")
                .name("Kafka Source");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaConfig.getBroker())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaConfig.getTestProducerTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                .setProperty(ProducerConfig.ACKS_CONFIG, "all")
                .setProperty(ProducerConfig.RETRIES_CONFIG, "3")
                .setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
                .setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,  "600000") //must smaller than broker config(transaction.timeout.ms): default 15min
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("test")
                .build();
        sourceStream.sinkTo(kafkaSink)
                .uid("Kafka Sink")
                .name("Kafka Sink");
        env.execute();
    }
}
