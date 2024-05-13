package gov.cdc.etldatapipeline.postprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.postprocessingservice.repository.InvestigationRepository;
import gov.cdc.etldatapipeline.postprocessingservice.repository.OrganizationRepository;
import gov.cdc.etldatapipeline.postprocessingservice.repository.PatientRepository;
import gov.cdc.etldatapipeline.postprocessingservice.repository.ProviderRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@EnableScheduling
public class PostProcessingService {
    private static final Logger logger = LoggerFactory.getLogger(PostProcessingService.class);
    final Map<String, List<Long>> idCache = new ConcurrentHashMap<>();

    private final PatientRepository patientRepository;
    private final ProviderRepository providerRepository;
    private final OrganizationRepository organizationRepository;
    private final InvestigationRepository investigationRepository;

    @KafkaListener(topics = {"${spring.kafka.topic.investigation}"})
    public void postProcessInvestigationMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Long id = extractIdFromMessage(key, topic);
        if(id != null) {
            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
        }
    }

//    @KafkaListener(topics = {"${spring.kafka.topic.investigation-confirmation}"})
//    public void postProcessInvestigationConfirmationMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
//                                                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
//        Long id = extractIdFromMessage(key, topic);
//        if(id != null) {
//            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
//        }
//    }
//
//    @KafkaListener(topics = {"${spring.kafka.topic.investigation-notification}"})
//    public void postProcessInvestigationNotificationMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
//                                                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
//        Long id = extractIdFromMessage(key, topic);
//        if(id != null) {
//            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
//        }
//    }

    @KafkaListener(topics = {"${spring.kafka.topic.organization}"})
    public void postProcessOrganizationMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Long id = extractIdFromMessage(key, topic);
        if(id != null) {
            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
        }
    }

    @KafkaListener(topics = {"${spring.kafka.topic.patient}"})
    public void postProcessPatientMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Long id = extractIdFromMessage(key, topic);
        if(id != null) {
            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
        }
    }

    @KafkaListener(topics = {"${spring.kafka.topic.provider}"})
    public void postProcessProviderMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Long id = extractIdFromMessage(key, topic);
        if(id != null) {
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
                if(keyTopic.contains("investigation")) {
                    String idsString = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
                    logger.info("Processing the ids from the topic {} and calling the stored proc for patient: {}", keyTopic, idsString);
                    investigationRepository.executeStoredProcForPublicHealthCaseIds(idsString);
                    logger.info("Stored proc execution completed.");
                }
                if(keyTopic.contains("organization")) {
                    String idsString = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
                    logger.info("Processing the ids from the topic {} and calling the stored proc for patient: {}", keyTopic, idsString);
                    organizationRepository.executeStoredProcForOrganizationIds(idsString);
                    logger.info("Stored proc execution completed.");
                }
                if(keyTopic.contains("patient")) {
                    String idsString = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
                    logger.info("Processing the ids from the topic {} and calling the stored proc for patient: {}", keyTopic, idsString);
                    patientRepository.executeStoredProcForPatientIds(idsString);
                    logger.info("Stored proc execution completed.");
                }
                if(keyTopic.contains("provider")) {
                    String idsString = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
                    logger.info("Processing the ids from the topic {} and calling the stored proc for patient: {}", keyTopic, idsString);
                    providerRepository.executeStoredProcForProviderIds(idsString);
                    logger.info("Stored proc execution completed.");
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
                id = jsonNode.get("payload").get("public_health_case_uid").asLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return id;
    }
}
