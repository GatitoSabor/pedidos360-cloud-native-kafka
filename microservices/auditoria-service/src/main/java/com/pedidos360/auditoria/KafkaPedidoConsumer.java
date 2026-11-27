package com.pedidos360.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/** Conserva trazabilidad de cada cambio de un pedido mediante el stream de eventos. */
@Service
public class KafkaPedidoConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaPedidoConsumer.class);
    @KafkaListener(topics = "pedidos.events.v1")
    public void audit(PedidoEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        if (event.descripcion().contains("[FALLAR-KAFKA]")) throw new IllegalStateException("Fallo Kafka de auditoría simulado");
        log.info("Kafka auditoría: pedido={} tipo={} partition={} offset={}", event.pedidoId(), event.tipo(), partition, offset);
    }
}
