package com.pedidos360.documentos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/** Genera la evidencia documental al recibir una decisión aprobada desde Kafka. */
@Service
public class KafkaPedidoConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaPedidoConsumer.class);
    @KafkaListener(topics = "pedidos.decisions.v1")
    public void generate(PedidoEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        if (event.descripcion().contains("[FALLAR-KAFKA]")) throw new IllegalStateException("Fallo Kafka de documento simulado");
        log.info("Kafka comprobante: pedido={} estado={} partition={} offset={}", event.pedidoId(), event.estado(), partition, offset);
    }
}
