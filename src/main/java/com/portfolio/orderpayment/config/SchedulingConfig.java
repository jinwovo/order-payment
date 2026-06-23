package com.portfolio.orderpayment.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Enables the @Scheduled outbox relay. */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
