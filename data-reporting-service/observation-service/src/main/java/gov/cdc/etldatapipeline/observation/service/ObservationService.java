package gov.cdc.etldatapipeline.observation.service;


import gov.cdc.etldatapipeline.observation.repository.IObservationRepository;
import gov.cdc.etldatapipeline.observation.repository.model.Observation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;


import java.util.Optional;

@Service
public class ObservationService {
    private final IObservationRepository iObservationRepository;

    private static final Logger logger = LoggerFactory.getLogger(ObservationService.class);

    @Value("${kafka.stream.input.observation.topic-name}")
    private String observationTopic = "cdc.NBS_ODSE.dbo.Observation";

    private String topicDebugLog = "Received message ID: {} from topic: {}";

    public ObservationService(IObservationRepository iObservationRepository) {
        this.iObservationRepository = iObservationRepository;
    }

    @KafkaListener(topics = "${kafka.stream.input.observation.topic-name}")
    public void processObservationUids(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.debug(topicDebugLog, message, topic);
        System.out.println("observationData class....inside");
        Optional<Observation> observationData = iObservationRepository.computePatients(message);
        System.out.println("observationData is..." + observationData);
    }
}