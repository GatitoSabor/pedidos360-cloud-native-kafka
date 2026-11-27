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

    // El group.id se define en application.properties: una instancia procesa cada partición por grupo.
    @KafkaListener(topics = "pedidos.events.v1")
    public void audit(PedidoEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        // Interruptor controlado para demostrar los reintentos sin provocar errores reales.
        if (event.descripcion().contains("[FALLAR-KAFKA]")) throw new IllegalStateException("Fallo Kafka de auditoría simulado");
        // Partición y offset hacen trazable el mensaje mostrado durante la presentación.
        log.info("Kafka auditoría: pedido={} tipo={} partition={} offset={}", event.pedidoId(), event.tipo(), partition, offset);
    }
}
