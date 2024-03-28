package gov.cdc.etldatapipeline.investigation.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.investigation.repository.InvestigationRepository;
import gov.cdc.etldatapipeline.investigation.repository.model.dto.Investigation;
import gov.cdc.etldatapipeline.investigation.repository.model.dto.InvestigationTransformed;
import gov.cdc.etldatapipeline.investigation.util.ProcessInvestigationDataUtil;
import lombok.RequiredArgsConstructor;
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
public class InvestigationService {
    private static final Logger logger = LoggerFactory.getLogger(InvestigationService.class);

    @Value("${spring.kafka.stream.input.investigation.topic-name}")
    private String investigationTopic = "cdc.nbs_odse.dbo.Investigation";

    @Value("${spring.kafka.stream.output.investigation.topic-name}")
    public String investigationTopicOutput = "cdc.nbs_odse.dbo.Investigation.output";

    @Value("${spring.kafka.stream.output.investigation.topic-name-transformed}")
    public String investigationTopicOutputTransformed = "cdc.nbs_odse.dbo.Investigation.output-transformed";

    private final InvestigationRepository investigationRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProcessInvestigationDataUtil processDataUtil;
    private String topicDebugLog = "Received Investigation ID: {} from topic: {}";

    private String investigationUid = "263771897";


    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        streamsBuilder.stream(investigationTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> v != null)
                .mapValues((key, value) -> processInvestigation(value))
                .filter((key, value) -> value != null)
                .to(investigationTopicOutput, Produced.with(Serdes.String(), Serdes.String()));
    }

    public String processInvestigation(String value) {
        try {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
//            JsonNode jsonNode = objectMapper.readTree(value);
//            JsonNode payloadNode = jsonNode.get("payload").path("after");
//            if (payloadNode != null && payloadNode.has("public_health_case_uid")) {
//                String publicHealthCaseUid = payloadNode.get("public_health_case_uid").asText();
//                logger.debug(topicDebugLog, publicHealthCaseUid, investigationTopic);
                Optional<Investigation> investigationData = investigationRepository.computeInvestigations(investigationUid);
                if(investigationData.isPresent()) {
                    InvestigationTransformed investigationTransformed = processDataUtil.transformInvestigationData(investigationData.get());
                    kafkaTemplate.send(investigationTopicOutputTransformed, investigationTransformed.toString());
                    return objectMapper.writeValueAsString(investigationData);
                }
//                return investigationData.map(investigation -> {
//                    try {
//
//                    } catch (JsonProcessingException e) {
//                        log.error("Error processing investigation: {}", e.getMessage());
//                        return null;
//                    }
//                }).orElse(null);
//            }
        } catch (Exception e) {
            logger.error("Error processing investigation: {}", e.getMessage());
        }
        return null;
    }



}
