package com.shaice.flink.config;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaConfig {
    private String broker;
    private String testConsumerTopic;
    private String testConsumerGroupId;
    private String testProducerTopic;

}
