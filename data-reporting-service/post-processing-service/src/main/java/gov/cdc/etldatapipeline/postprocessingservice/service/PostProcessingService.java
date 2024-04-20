package gov.cdc.etldatapipeline.postprocessingservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.postprocessingservice.repository.PatientRepository;
import gov.cdc.etldatapipeline.postprocessingservice.repository.TestRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

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

    @Value("${spring.application.batch-size}")
    private int BATCH_SIZE = 5;
    private final Map<String, List<Long>> idCache = new ConcurrentHashMap<>();

    private final PatientRepository patientRepository;
    private final TestRepository testRepository;

//    @KafkaListener(topics = "${spring.kafka.topic.investigation}")
//    public void postProcessInvestigationData(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)  {
//    }
//
//    @KafkaListener(topics = "${spring.kafka.topic.observation}")
//    public void postProcessObservationData(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)  {
//    }
//
//    @KafkaListener(topics = "${spring.kafka.topic.organization}")
//    public void postProcessOrganizationData(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)  {
//    }
//
//    @KafkaListener(topics = "${spring.kafka.topic.patient}")
//    public void postProcessPatientData(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)  {
////        String uidTag = "patient_uid";
////        Long id = extractIdFromMessage(key, topic, uidTag);
////        if(id != null) {
////            addIdToCache(topic, id);
////        idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
////        }
//
//        System.err.println("Printing inside individual listener....");
//    }

    @KafkaListener(topics = {"${spring.kafka.topic.investigation}",
            "${spring.kafka.topic.observation}",
            "${spring.kafka.topic.organization}",
            "${spring.kafka.topic.patient}"})
    public void postProcessKafkaMessage(String message,
                                        @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) throws JsonProcessingException {
        Long id = extractIdFromMessage(key, topic);
        System.err.println("Printing inside common listener...." + id.toString());
        if(id != null) {
            idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
        }
        System.err.println("Printing idCache map..." + idCache);
    }

    @Transactional
    @Scheduled(fixedDelay = 5000)
    protected void processCachedIds() {

        System.err.println("Running scheduled method... with batch size...." + idCache.size());


        for (Map.Entry<String, List<Long>> entry : idCache.entrySet()) {
            String keyTopic = entry.getKey();
            List<Long> ids = entry.getValue();

//            if (ids.size() >= BATCH_SIZE) {
                System.err.println("Printing ids..." + ids);
                System.err.println("Printing ids size..." + ids.size());
//                if(keyTopic.contains("observation")) {
//                    List<ObservationStoredProc> results = null;
//                    for(ObservationStoredProc data : results) {
////                        observationRepository.save(data);
////                        idCache.remove(topic);
//                    }
//                }
//                if(keyTopic.contains("organization")) {
//                    List<InvestigationStoredProc> results = null;
//                    for(InvestigationStoredProc data : results) {
////                        organizationRepository.save(data);
////                        idCache.remove(topic);
//                    }
//                }
//                if(keyTopic.contains("investigation")) {
//                    List<InvestigationStoredProc> results = null;
//                    for(InvestigationStoredProc data : results) {
////                        investigationRepository.save(data);
////                        idCache.remove(topic);
//                    }
//                }
                if(keyTopic.contains("patient")) {
                    String idsString = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
                    System.err.println("Ids string is...." + idsString);
//                    List<TestModel> results = testRepository.computeIds(idsString);
                    //List<PatientStoredProc> results = patientRepository.executeStoredProcForPatientIds(idsString);
                    patientRepository.executeStoredProcForPatientIds(idsString);
//                    System.err.println("Printing computed results..." + results);
//                    if(!results.isEmpty()) {
//                        for(PatientStoredProc data : results) {
//                            patientRepository.save(data);
//                            idCache.remove(topic);
//                        }
//                        for(TestModel data : results) {
//                            System.err.println("Saving test model to database..." + data.toString());
//                        }
                        idCache.remove(keyTopic);
//                    }
//                    else {
//                        log.error("Stored Proc returned empty results.");
//                        System.err.println("Stored Proc returned empty results.");
                        //TODO: Send message to DLT for further investigation
//                    }
//                }
            }
        }
    }

    private void addIdToCache(String topic, Long id) {
        idCache.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(id);
    }

    private Long extractIdFromMessage(String messageKey, String topic) {
        Long id = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            JsonNode jsonNode = objectMapper.readTree(messageKey);
            if(topic.contains("observation")) {
                id = jsonNode.get("payload").get("observation_uid").asLong();
            }
            if(topic.contains("organization")) {
                id = jsonNode.get("payload").get("organization_uid").asLong();
            }
            if(topic.contains("investigation")) {
                id = jsonNode.get("payload").get("public_health_case_uid").asLong();
            }
            if(topic.contains("patient")) {
                id = jsonNode.get("payload").get("patient_uid").asLong();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return id;
    }
}
