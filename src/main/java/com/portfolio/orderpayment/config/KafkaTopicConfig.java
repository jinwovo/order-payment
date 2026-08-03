package com.portfolio.orderpayment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Declares the outbox destination topic and its DLT; KafkaAdmin creates them on startup if absent. */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic(@Value("${outbox.topic:order-events}") String topic) {
        return new NewTopic(topic, 1, (short) 1);
    }

    // Same partition count as the source topic: the DLT recoverer routes to the same partition.
    @Bean
    public NewTopic orderEventsDltTopic(@Value("${outbox.topic:order-events}") String topic) {
        return new NewTopic(topic + ".DLT", 1, (short) 1);
    }
}
