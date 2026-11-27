package com.pedidos360.backend.messaging;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Define tópicos versionables y sus políticas de conservación para Pedidos360. */
@Configuration
public class KafkaTopicConfig {
    // El sufijo v1 permite evolucionar el contrato sin romper consumidores existentes.
    public static final String PEDIDO_EVENTS_TOPIC = "pedidos.events.v1";
    public static final String PEDIDO_DECISIONS_TOPIC = "pedidos.decisions.v1";
    // La evaluación solicita tres particiones y tres réplicas, una por broker.
    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 3;

    @Bean
    NewTopic pedidoEventsTopic() {
        return topic(PEDIDO_EVENTS_TOPIC, "604800000"); // 7 días: trazabilidad operativa.
    }

    @Bean
    NewTopic pedidoDecisionsTopic() {
        return topic(PEDIDO_DECISIONS_TOPIC, "2592000000"); // 30 días: decisiones auditables.
    }

    private NewTopic topic(String name, String retentionMs) {
        return TopicBuilder.name(name)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .configs(Map.of(
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                        TopicConfig.RETENTION_MS_CONFIG, retentionMs,
                        // Con tres réplicas, exige dos ISR antes de confirmar la escritura.
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))
                .build();
    }
}
