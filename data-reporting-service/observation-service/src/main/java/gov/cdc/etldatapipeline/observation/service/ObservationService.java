package gov.cdc.etldatapipeline.observation.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.observation.repository.IObservationRepository;
import gov.cdc.etldatapipeline.observation.repository.model.Observation;
import gov.cdc.etldatapipeline.observation.repository.model.ObservationTransformed;
import gov.cdc.etldatapipeline.observation.util.ProcessObservationDataUtil;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
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
@Setter
@RequiredArgsConstructor
public class ObservationService {
    private static final Logger logger = LoggerFactory.getLogger(ObservationService.class);

    @Value("${spring.kafka.stream.input.observation.topic-name}")
    private String observationTopic = "cdc.nbs_odse.dbo.Observation";

    @Value("${spring.kafka.stream.output.observation.topic-name}")
    public String observationTopicOutput = "cdc.nbs_odse.dbo.Observation.output";

    @Value("${spring.kafka.stream.output.observation.topic-name-transformed}")
    public String observationTopicOutputTransformed = "cdc.nbs_odse.dbo.Observation.output-transformed";

    private final IObservationRepository iObservationRepository;
    private String topicDebugLog = "Received Observation ID: {} from topic: {}";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProcessObservationDataUtil processObservationDataUtil;


    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        streamsBuilder.stream(observationTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> v != null)
                .mapValues((key, value) -> processObservation(value))
                .filter((key, value) -> value != null)
                .to(observationTopicOutput, Produced.with(Serdes.String(), Serdes.String()));
    }

    private String processObservation(String value) {
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
                    Optional<Observation> observationData = iObservationRepository.computeObservations(observationUid);
                    if(observationData.isPresent()) {
                        ObservationTransformed observationTransformed = processObservationDataUtil.transformObservationData(observationData.get());
                        kafkaTemplate.send(observationTopicOutputTransformed, observationTransformed.toString());
                    }
                    return objectMapper.writeValueAsString(observationData.get());
                }
            }
        } catch (Exception e) {
            logger.error("Error processing observation: {}", e.getMessage());
        }
        return null;
    }
}
