package gov.cdc.etldatapipeline.observation.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.observation.repository.IObservationRepository;
import gov.cdc.etldatapipeline.observation.repository.model.Observation;
import gov.cdc.etldatapipeline.observation.repository.model.ObservationTransformed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ObservationService {
    private final IObservationRepository iObservationRepository;

    private static final Logger logger = LoggerFactory.getLogger(ObservationService.class);

    @Value("${spring.kafka.stream.input.observation.topic-name}")
    private String observationTopic = "cdc.nbs_odse.dbo.Observation";

    @Value("${spring.kafka.stream.output.observation.topic-name}")
    public String observationTopicOutput = "cdc.nbs_odse.dbo.Observation.output";

    @Value("${spring.kafka.stream.output.observation.topic-name-transformed}")
    public String observationTopicOutputTransformed = "cdc.nbs_odse.dbo.Observation.output-transformed";

    private String topicDebugLog = "Received Observation ID: {} from topic: {}";

//    Optional<Observation> observationData;

    ObservationTransformed observationTransformed;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        streamsBuilder.stream(observationTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> v != null)
                .mapValues((key, value) -> {
                    try {
                        ObjectMapper objectMapper = new ObjectMapper();
                        objectMapper.registerModule(new JavaTimeModule());
                        JsonNode jsonNode = objectMapper.readTree(value);
                        JsonNode payloadNode = jsonNode.get("payload");
                        if (payloadNode != null && payloadNode.has("after")) {
                            JsonNode afterNode = payloadNode.get("after");
                            if (afterNode != null && afterNode.has("observation_uid")) {
                                String observationUid = afterNode.get("observation_uid").asText();
                                logger.debug(topicDebugLog, observationUid, observationTopic);
                                System.err.println("Received Observation ID..."+observationUid+"...from topic..."+ observationTopic);
                                Optional<Observation> observationData = iObservationRepository.computeObservations(observationUid);
                                transformObservationData(observationData);
                                return observationData.map(observation -> {
                                    try {
                                        return objectMapper.writeValueAsString(observation);
                                    } catch (JsonProcessingException e) {
                                        e.printStackTrace();
                                        return null;
                                    }
                                }).orElse(null);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .filter((key, value) -> value != null)
                .to(observationTopicOutput, Produced.with(Serdes.String(), Serdes.String()));
    }

    private void transformObservationData(Optional<Observation> observationData) {
        observationData.ifPresent(data -> {
        String personParticipationsJson = data.getPersonParticipations();
        String organizationParticipationsJson = data.getOrganizationParticipations();
        String materialParticipationsJson = data.getMaterialParticipations();
        String followupObservationsJson = data.getFollowupObservations();
        String observationDomainStatusCode = data.getObsDomainCdSt1();

        observationTransformed = new ObservationTransformed();

//        if(observationDomainStatusCode.equals("Order")) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode personParticipationsJsonArray = personParticipationsJson != null ? objectMapper.readTree(personParticipationsJson) : null;
                JsonNode organizationParticipationsJsonArray = organizationParticipationsJson != null ? objectMapper.readTree(organizationParticipationsJson) : null;
                JsonNode materialParticipationsJsonArray = materialParticipationsJson != null ? objectMapper.readTree(materialParticipationsJson) : null;
                JsonNode followupObservationsJsonArray = followupObservationsJson != null ? objectMapper.readTree(followupObservationsJson) : null;


                if(personParticipationsJson != null && personParticipationsJsonArray.isArray()) {
                    for(JsonNode jsonNode : personParticipationsJsonArray) {
                        processJson(jsonNode, observationTransformed);
                    }
                }
                if(organizationParticipationsJson != null && organizationParticipationsJsonArray.isArray()) {
                    for(JsonNode jsonNode : organizationParticipationsJsonArray) {
                        processJson(jsonNode, observationTransformed);
                    }
                }
                if(materialParticipationsJson != null && materialParticipationsJsonArray.isArray()) {
                    for(JsonNode jsonNode : materialParticipationsJsonArray) {
                        processJson(jsonNode, observationTransformed);
                    }
                }
                if(followupObservationsJson != null && followupObservationsJsonArray.isArray()) {
                    for(JsonNode jsonNode : followupObservationsJsonArray) {
                        processJson(jsonNode, observationTransformed, observationDomainStatusCode);
                    }
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

//        }
//        else if(observationDomainStatusCode.equals("Result")) {
//
//        }
//        else {
//            logger.error("Unknown Observation Domain Status Code found: {}", observationDomainStatusCode);
//        }
        System.err.println("transformed obj is..." + observationTransformed.toString());
        kafkaTemplate.send(observationTopicOutputTransformed, observationTransformed.toString());
        });
    }

    private void processJson(JsonNode jsonNode, ObservationTransformed observationTransformed) {
        String typeCode = jsonNode.get("type_cd").asText();
        String subjectClassCode = jsonNode.get("subject_class_cd").asText();
//        String domainCdSt1 = String.valueOf(jsonNode.get("domain_cd_st_1"));
        //String observationDomainStatusCode = observationData.get().getObsDomainCdSt1();


        if(typeCode != null && subjectClassCode != null &&
                typeCode.equals("ORD") && subjectClassCode.equals("PSN")) {
            observationTransformed.setOrderingOrganizationId(String.valueOf(jsonNode.get("entity_id")));
        }
        if (typeCode != null && subjectClassCode != null &&
                typeCode.equals("PATSBJ") && subjectClassCode.equals("PSN")) {
            observationTransformed.setPatientId(String.valueOf(jsonNode.get("entity_id")));
        }
        if (typeCode != null && subjectClassCode != null &&
                typeCode.equals("PRF") && subjectClassCode.equals("ORG")) {
            observationTransformed.setPerformingOrganizationId(String.valueOf(jsonNode.get("entity_id")));
        }
        if (typeCode != null && subjectClassCode != null &&
                typeCode.equals("AUT") && subjectClassCode.equals("ORG")) {
            observationTransformed.setAuthorOrganizationId(String.valueOf(jsonNode.get("entity_id")));
        }
        if (typeCode != null && subjectClassCode != null &&
                typeCode.equals("ORD") && subjectClassCode.equals("ORG")) {
            observationTransformed.setOrderingOrganizationId(String.valueOf(jsonNode.get("entity_id")));
        }
        if (typeCode != null && subjectClassCode != null &&
                typeCode.equals("SPC") && subjectClassCode.equals("MAT")) {
            observationTransformed.setMaterialId(String.valueOf(jsonNode.get("entity_id")));
        }
    }

    private void processJson(JsonNode jsonNode, ObservationTransformed observationTransformed, String observationDomainStatusCode) {
        String domainCdSt1 = jsonNode.get("domain_cd_st_1").asText();

        if (observationDomainStatusCode.equalsIgnoreCase("Order") && domainCdSt1 != null && domainCdSt1.equals("Result")) {
            observationTransformed.setResultObservationUid(jsonNode.get("result_observation_uid").asText());
        }
    }
}