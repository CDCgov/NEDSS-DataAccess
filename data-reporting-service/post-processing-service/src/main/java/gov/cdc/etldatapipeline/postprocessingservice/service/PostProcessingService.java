package gov.cdc.etldatapipeline.postprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.postprocessingservice.repository.*;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@EnableScheduling
public class PostProcessingService {
    private static final Logger logger = LoggerFactory.getLogger(PostProcessingService.class);
    final Map<String, List<Long>> idCache = new ConcurrentHashMap<>();
    final Map<Long, String> idVals = new ConcurrentHashMap<>();

    private final PatientRepository patientRepository;
    private final ProviderRepository providerRepository;
    private final OrganizationRepository organizationRepository;
    private final InvestigationRepository investigationRepository;
    private final NotificationRepository notificationRepository;
    private final PageBuilderRepository pageBuilderRepository;

    static final String SP_EXECUTION_COMPLETED = "Stored proc execution completed.";

    @KafkaListener(topics = {
            "${spring.kafka.topic.investigation}",
            "${spring.kafka.topic.organization}",
            "${spring.kafka.topic.patient}",
            "${spring.kafka.topic.provider}",
            "${spring.kafka.topic.notification}"
    })
    public void postProcessMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Long id = extractIdFromMessage(key, topic);
        if (id != null) {
            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
        }
    }

    @Scheduled(fixedDelay = 5000)
    protected void processCachedIds() {
        for (Map.Entry<String, List<Long>> entry : idCache.entrySet()) {
            if(!entry.getValue().isEmpty()) {
                String keyTopic = entry.getKey();
                List<Long> ids = entry.getValue();
                idCache.put(keyTopic, new ArrayList<>());
                if(keyTopic.contains("organization")) {
                    processTopic(keyTopic, ids, organizationRepository::executeStoredProcForOrganizationIds, "organization", "sp_nrt_organization_postprocessing");
                }
                if(keyTopic.contains("provider")) {
                    processTopic(keyTopic, ids, providerRepository::executeStoredProcForProviderIds, "provider", "sp_nrt_provider_postprocessing");
                }
                if(keyTopic.contains("patient")) {
                    processTopic(keyTopic, ids, patientRepository::executeStoredProcForPatientIds, "patient", "sp_nrt_patient_postprocessing");
                }
                if(keyTopic.contains("investigation")) {
                    processTopic(keyTopic, ids, investigationRepository::executeStoredProcForPublicHealthCaseIds, "investigation","sp_nrt_investigation_postprocessing");
                    ids.forEach(id -> {
                        if (idVals.containsKey(id)) {
                            processId(id, idVals.get(id), pageBuilderRepository::executeStoredProcForPageBuilder, "case answers","sp_page_builder_postprocessing");
                        }
                    });
                    processTopic(keyTopic, ids, investigationRepository::executeStoredProcForFPageCase, "investigation","sp_f_page_case_postprocessing");
                }
                if(keyTopic.contains("notifications")) {
                    processTopic(keyTopic, ids, notificationRepository::executeStoredProcForNotificationIds, "notifications", "sp_nrt_notification_postprocessing");
                }
            }
            else {
                logger.info("No ids to process from the topics.");
            }
        }
    }

    Long extractIdFromMessage(String messageKey, String topic) {
        Long id = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            JsonNode jsonNode = objectMapper.readTree(messageKey);
            logger.info("Got this key payload: {} from the topic: {}", messageKey, topic);
            if(topic.contains("patient")) {
                id = jsonNode.get("payload").get("patient_uid").asLong();
            }
            if(topic.contains("provider")) {
                id = jsonNode.get("payload").get("provider_uid").asLong();
            }
            if(topic.contains("organization")) {
                id = jsonNode.get("payload").get("organization_uid").asLong();
            }
            if(topic.contains("investigation")) {
                final Long phcUid = id = jsonNode.get("payload").get("public_health_case_uid").asLong();
                Optional.ofNullable(jsonNode.get("payload").get("rdb_table_name_list"))
                        .ifPresent(node -> idVals.put(phcUid, node.asText()));
            }
            if(topic.contains("notifications")) {
                id = jsonNode.get("payload").get("source_act_uid").asLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    private void processTopic(String keyTopic, List<Long> ids, Consumer<String> repositoryMethod, String entity, String proc) {
        if(keyTopic.contains(entity)) {
            String idsString = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
            logger.info("Processing the {} message topic: {}. Calling stored proc: {}('{}')", entity, keyTopic, proc, idsString);
            repositoryMethod.accept(idsString);
            logger.info(SP_EXECUTION_COMPLETED);
        }
    }

    private void processId(Long id, String vals,BiConsumer<Long, String> repositoryMethod, String entity, String proc) {
            logger.info("Processing PHC ID for {}. Calling stored proc: {}({}, '{}')", entity, proc, id, vals);
            repositoryMethod.accept(id, vals);
            logger.info(SP_EXECUTION_COMPLETED);
    }
}
