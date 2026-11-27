package com.pedidos360.kafkaadmin;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.Map;

public record TopicRequest(
        @NotBlank @Pattern(regexp = "^[a-z0-9][a-z0-9.-]{1,80}$") String name,
        @Min(1) @Max(100) int partitions,
        @Min(1) @Max(3) short replicationFactor,
        Map<String, String> configs) { }
