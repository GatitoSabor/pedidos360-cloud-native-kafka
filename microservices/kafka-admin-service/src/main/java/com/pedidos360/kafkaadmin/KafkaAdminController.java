package com.pedidos360.kafkaadmin;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** API administrativa para crear, listar, configurar y eliminar tópicos Kafka. */
@RestController
@Validated
@RequestMapping("/api/kafka")
public class KafkaAdminController {
    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @PostMapping("/topics")
    ResponseEntity<Void> createTopic(@Valid @RequestBody TopicRequest request) throws Exception {
        kafkaAdminService.createTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/topics")
    List<KafkaAdminService.TopicSummary> listTopics() throws Exception {
        return kafkaAdminService.listTopics();
    }

    @PutMapping("/topics/{name}/configs")
    ResponseEntity<Void> updateTopicConfig(@PathVariable @Pattern(regexp = "^[a-z0-9][a-z0-9.-]{1,80}$") String name,
            @Valid @RequestBody TopicConfigRequest request) throws Exception {
        kafkaAdminService.updateTopicConfig(name, request);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/topics/{name}")
    ResponseEntity<Void> deleteTopic(@PathVariable @Pattern(regexp = "^[a-z0-9][a-z0-9.-]{1,80}$") String name) throws Exception {
        kafkaAdminService.deleteTopic(name);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/consumer-groups")
    List<String> listConsumerGroups() throws Exception {
        return kafkaAdminService.listConsumerGroups();
    }
}
