package com.pedidos360.notificaciones;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/** Consume decisiones finales para informar cambios relevantes a las personas usuarias. */
@Service
public class KafkaPedidoConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaPedidoConsumer.class);

    @KafkaListener(topics = "pedidos.decisions.v1")
    public void notify(PedidoEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        if (event.descripcion().contains("[FALLAR-KAFKA]")) {
            throw new IllegalStateException("Fallo Kafka de notificación simulado");
        }
        log.info("Kafka notificación: pedido={} estado={} partition={} offset={}",
                event.pedidoId(), event.estado(), partition, offset);
    }
}
