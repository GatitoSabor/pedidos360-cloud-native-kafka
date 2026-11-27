package com.pedidos360.kafkaadmin;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/** Total final de particiones solicitado para un tópico existente; solo puede aumentar. */
public record TopicPartitionsRequest(
        @Min(1) @Max(100) int totalPartitions) { }
