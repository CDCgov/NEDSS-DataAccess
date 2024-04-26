package gov.cdc.etldatapipeline.postprocessingservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.postprocessingservice.repository.PatientRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
@RequiredArgsConstructor
@EnableScheduling
public class PostProcessingService {
    private final Map<String, List<Long>> idCache = new ConcurrentHashMap<>();

    private final PatientRepository patientRepository;

    @KafkaListener(topics = {"${spring.kafka.topic.investigation}",
            "${spring.kafka.topic.observation}",
            "${spring.kafka.topic.organization}",
            "${spring.kafka.topic.patient}"})
    public void postProcessKafkaMessage(String message,
                                        @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Long id = extractIdFromMessage(key, topic);
        if(id != null) {
            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
        }
    }

    @Transactional
    @Scheduled(fixedDelay = 5000)
    protected void processCachedIds() {
        for (Map.Entry<String, List<Long>> entry : idCache.entrySet()) {
            String keyTopic = entry.getKey();
            List<Long> ids = entry.getValue();
            idCache.put(keyTopic, new ArrayList<>());
            if(keyTopic.contains("patient")) {
                String idsString = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
                log.info("Processing the ids from the topic {} and calling the stored proc for patient: {}", keyTopic, idsString);
                patientRepository.executeStoredProcForPatientIds(idsString);
                log.info("Stored proc execution completed.");
            }
        }
    }

    private Long extractIdFromMessage(String messageKey, String topic) {
        Long id = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            JsonNode jsonNode = objectMapper.readTree(messageKey);
            log.info("Got this key payload: {} from the topic: {}", messageKey, topic);
            if(topic.contains("patient")) {
                id = jsonNode.get("payload").get("patient_uid").asLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return id;
    }
}
