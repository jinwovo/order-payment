package com.portfolio.orderpayment;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

/**
 * A real broker for the pipeline tests, running the exact KRaft config docker-compose uses. The
 * advertised listener must be a host-reachable address known before startup, hence the fixed host
 * port. Only the Kafka-touching suites import this — the pure-saga tests stay broker-free and fast.
 */
@TestConfiguration(proxyBeanMethods = false)
class KafkaTestcontainers {

	static final int KAFKA_HOST_PORT = 19093;

	// FixedHostPortGenericContainer is deprecated in favour of random mapped ports, but a Kafka
	// broker must ADVERTISE a host-reachable address it cannot know before startup — the exact
	// case fixed host ports exist for. The suppressed warning is the honest trade.
	@Bean
	@SuppressWarnings({"deprecation", "resource"})
	GenericContainer<?> kafkaContainer() {
		return new FixedHostPortGenericContainer<>("apache/kafka:3.9.0")
				.withFixedExposedPort(KAFKA_HOST_PORT, 9092)
				.withEnv("KAFKA_NODE_ID", "1")
				.withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
				// The external listener advertises the mapped host port, which does not exist
				// inside the container — so the broker gets its own internal listener (BROKER)
				// for self/inter-broker traffic, or topic metadata never materializes.
				// Empty listener hosts on purpose: this image's storage-format step rejects
				// 0.0.0.0 here and the container dies instantly.
				.withEnv("KAFKA_LISTENERS", "PLAINTEXT://:9092,BROKER://:9094,CONTROLLER://:9093")
				.withEnv("KAFKA_ADVERTISED_LISTENERS",
						"PLAINTEXT://localhost:" + KAFKA_HOST_PORT + ",BROKER://localhost:9094")
				.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
				.withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
				.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
						"CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT")
				.withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
				.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
				.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
				.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
				.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
				.withEnv("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m")
				// Port-based wait on purpose: log-stream waits hang on this Docker setup (Windows
				// named-pipe log streaming), and the broker binding its listener is signal enough —
				// Kafka clients retry through the last seconds of broker recovery on their own.
				.waitingFor(Wait.forListeningPort())
				.withStartupTimeout(Duration.ofMinutes(3));
	}

}
