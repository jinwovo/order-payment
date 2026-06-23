package com.portfolio.orderpayment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Declares the outbox destination topic; KafkaAdmin creates it on startup if absent. */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic(@Value("${outbox.topic:order-events}") String topic) {
        return new NewTopic(topic, 1, (short) 1);
    }
}
