package com.pedidos360.documentos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Reintentos limitados para que un documento fallido no detenga el flujo de eventos. */
@Configuration
public class KafkaRetryConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaRetryConfig.class);
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PedidoEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, PedidoEvent> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, PedidoEvent>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception error) -> log.error("Kafka descartó topic={} partition={} offset={}", record.topic(), record.partition(), record.offset(), error),
                new FixedBackOff(1000L, 3L)));
        return factory;
    }
}
