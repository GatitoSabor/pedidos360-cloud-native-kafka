package com.pedidos360.kafkaadmin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.stereotype.Service;

/** Operaciones reproducibles sobre tópicos y grupos, aisladas de la API HTTP. */
@Service
public class KafkaAdminService {
    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public void createTopic(TopicRequest request) throws Exception {
        // El factor de réplica se envía explícitamente para no depender del valor por defecto del broker.
        NewTopic topic = new NewTopic(request.name(), request.partitions(), request.replicationFactor());
        if (request.configs() != null && !request.configs().isEmpty()) topic.configs(request.configs());
        adminClient.createTopics(List.of(topic)).all().get();
    }

    public List<TopicSummary> listTopics() throws Exception {
        var names = adminClient.listTopics().names().get();
        var descriptions = adminClient.describeTopics(names).allTopicNames().get();
        return descriptions.values().stream()
                .map(topic -> new TopicSummary(topic.name(), topic.partitions().size(),
                        topic.partitions().isEmpty() ? 0 : topic.partitions().get(0).replicas().size()))
                .sorted(java.util.Comparator.comparing(TopicSummary::name))
                .toList();
    }

    public void updateTopicConfig(String topicName, TopicConfigRequest request) throws Exception {
        // incrementalAlterConfigs cambia solo las claves recibidas y conserva el resto de la configuración.
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Collection<AlterConfigOp> operations = request.configs().entrySet().stream()
                .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        adminClient.incrementalAlterConfigs(Map.of(resource, operations)).all().get();
    }

    /** Kafka permite ampliar un tópico; reducir sus particiones no está soportado. */
    public void increasePartitions(String topicName, TopicPartitionsRequest request) throws Exception {
        int currentPartitions = adminClient.describeTopics(List.of(topicName)).allTopicNames().get()
                .get(topicName).partitions().size();
        if (request.totalPartitions() <= currentPartitions) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "El total debe ser mayor que las " + currentPartitions + " particiones existentes");
        }
        adminClient.createPartitions(Map.of(topicName, NewPartitions.increaseTo(request.totalPartitions()))).all().get();
    }

    public void deleteTopic(String topicName) throws Exception {
        // Operación administrativa: se ejecuta de forma explícita desde la API, nunca al iniciar el servicio.
        adminClient.deleteTopics(List.of(topicName)).all().get();
    }

    public List<String> listConsumerGroups() throws Exception {
        return adminClient.listConsumerGroups().all().get().stream()
                .map(group -> group.groupId()).sorted().toList();
    }

    public record TopicSummary(String name, int partitions, int replicationFactor) { }
}
