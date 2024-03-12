package gov.cdc.datareportingservice.kafka;


import gov.cdc.datareportingservice.observation.service.ObservationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Value("${kafka.stream.input.organization.topic-name}")
    private String organizationTopic = "cdc.NBS_ODSE.dbo.Organization";

    private String topicDebugLog = "Received message ID: {} from topic: {}";

    private final ObservationService observationService;

    public KafkaConsumerService(ObservationService observationService) {
        this.observationService = observationService;
    }

    @KafkaListener(topics = "${kafka.stream.input.organization.topic-name}")
    public void handleMessageFromObservationTopic(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.debug(topicDebugLog, message, topic);
        observationService.processObservationIds(message);
    }

}
