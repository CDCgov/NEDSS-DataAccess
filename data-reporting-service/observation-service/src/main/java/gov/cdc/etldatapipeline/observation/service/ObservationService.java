package gov.cdc.etldatapipeline.observation.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.commonutil.json.CustomJsonGeneratorImpl;
import gov.cdc.etldatapipeline.observation.repository.IObservationRepository;
import gov.cdc.etldatapipeline.observation.repository.model.dto.Observation;
import gov.cdc.etldatapipeline.observation.repository.model.dto.ObservationKey;
import gov.cdc.etldatapipeline.observation.repository.model.reporting.ObservationReporting;
import gov.cdc.etldatapipeline.observation.repository.model.dto.ObservationTransformed;
import gov.cdc.etldatapipeline.observation.util.ProcessObservationDataUtil;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.modelmapper.ModelMapper;
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
    private String observationTopic;

    @Value("${spring.kafka.stream.output.observation.topic-name-reporting}")
    public String observationTopicOutputReporting;

    @Value("${spring.kafka.stream.output.observation.topic-name-es}")
    public String observationTopicOutputElasticSearch;

    private final IObservationRepository iObservationRepository;
    private String topicDebugLog = "Received Observation ID: {} from topic: {}";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProcessObservationDataUtil processObservationDataUtil;
    ObservationKey observationKey = new ObservationKey();
    private final ModelMapper modelMapper = new ModelMapper();
    private final CustomJsonGeneratorImpl jsonGenerator = new CustomJsonGeneratorImpl();


    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        streamsBuilder.stream(observationTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> v != null)
                .mapValues((key, value) -> processObservation(value))
                .filter((key, value) -> value != null)
                .peek((key, value) -> logger.info("Received Observation : " + value));
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
                    observationKey.setObservationUid(Long.valueOf(observationUid));
                    logger.debug(topicDebugLog, observationUid, observationTopic);
                    Optional<Observation> observationData = iObservationRepository.computeObservations(observationUid);
                    if(observationData.isPresent()) {
                        ObservationReporting reportingModel = modelMapper.map(observationData.get(), ObservationReporting.class);
                        ObservationTransformed observationTransformed = processObservationDataUtil.transformObservationData(observationData.get());
                        buildReportingModelForTransformedData(reportingModel, observationTransformed);
                        pushKeyValuePairToKafka(observationKey, reportingModel, observationTopicOutputReporting);
                        return objectMapper.writeValueAsString(observationData.get());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing observation: {}", e.getMessage());
        }
        return null;
    }

    // This same method can be used for elastic search as well and that is why the generic model is present
    private void pushKeyValuePairToKafka(ObservationKey observationKey, Object model, String topicName) {
        String jsonKey = jsonGenerator.generateStringJson(observationKey);
        String jsonValue = jsonGenerator.generateStringJson(model);
        kafkaTemplate.send(topicName, jsonKey, jsonValue);
    }

    protected void buildReportingModelForTransformedData(ObservationReporting reportingModel, ObservationTransformed observationTransformed) {
        reportingModel.setOrderingPersonId(observationTransformed.getOrderingPersonId());
        reportingModel.setPatientId(observationTransformed.getPatientId());
        reportingModel.setPerformingOrganizationId(observationTransformed.getPerformingOrganizationId());
        reportingModel.setAuthorOrganizationId(observationTransformed.getAuthorOrganizationId());
        reportingModel.setOrderingOrganizationId(observationTransformed.getOrderingOrganizationId());
        reportingModel.setMaterialId(observationTransformed.getMaterialId());
        reportingModel.setResultObservationUid(observationTransformed.getResultObservationUid());
    }
}
