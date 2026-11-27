package com.pedidos360.backend.messaging;

import java.time.Instant;
import com.pedidos360.backend.entity.Pedido;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
/** Publica cambios de pedidos sin acoplar el controlador a los consumidores. */
public class PedidoEventProducer {
    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, PedidoEvent> kafkaTemplate;

    public PedidoEventProducer(RabbitTemplate rabbitTemplate, KafkaTemplate<String, PedidoEvent> kafkaTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Serializa el estado relevante del pedido y lo publica con la routing key
     * recibida, por ejemplo pedido.creado o pedido.aprobado.
     */
    public void publish(Pedido pedido, String eventType) {
        PedidoEvent event = new PedidoEvent(pedido.getId(), eventType, pedido.getDescripcion(), pedido.getEstado(), Instant.now());
        // RabbitMQ se conserva para el flujo anterior de colas y DLQ.
        rabbitTemplate.convertAndSend(RabbitTopologyConfig.EVENTS_EXCHANGE, eventType, event);
        // pedidoId como key mantiene el orden de eventos de un mismo pedido en una partición.
        kafkaTemplate.send(KafkaTopicConfig.PEDIDO_EVENTS_TOPIC, pedido.getId().toString(), event);
        if ("pedido.aprobado".equals(eventType) || "pedido.rechazado".equals(eventType)) {
            // Las decisiones tienen un stream separado para consumidores interesados en el resultado.
            kafkaTemplate.send(KafkaTopicConfig.PEDIDO_DECISIONS_TOPIC, pedido.getId().toString(), event);
        }
    }
}
