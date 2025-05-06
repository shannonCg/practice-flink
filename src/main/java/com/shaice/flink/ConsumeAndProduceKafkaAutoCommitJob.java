package com.shaice.flink;

import com.shaice.flink.config.KafkaConfig;
import com.shaice.flink.util.ParameterToolUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ConsumeAndProduceKafkaAutoCommitJob {
    public void startJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterToolUtil parameterToolUtil = new ParameterToolUtil();
        KafkaConfig kafkaConfig = parameterToolUtil.getKafkaConfig();
        KafkaSource<String> kafkaConsumer = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaConfig.getBroker())
                .setTopics(kafkaConfig.getTestConsumerTopic())
                .setGroupId(kafkaConfig.getTestConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) //consume from the earliest offset if no offset record store in checkpoint, else consume from the recorded offset
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "false") //close commit offset on checkpoint
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") // enable auto commit
                .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // set the offset reset strategy
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000") // set the auto commit interval(decide how often to commit offsets)
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaConfig.getBroker())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaConfig.getTestProducerTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                .setProperty(ProducerConfig.ACKS_CONFIG, "1")
                .setProperty(ProducerConfig.RETRIES_CONFIG, "3")
                .setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
                .build();
        sourceStream.sinkTo(kafkaSink);
        env.execute();
    }
}
