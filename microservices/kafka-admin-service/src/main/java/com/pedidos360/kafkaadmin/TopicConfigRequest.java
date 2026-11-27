package com.pedidos360.kafkaadmin;

import jakarta.validation.constraints.NotEmpty;
import java.util.Map;

public record TopicConfigRequest(@NotEmpty Map<String, String> configs) { }
