package gov.cdc.etldatapipeline.investigation.config;

import gov.cdc.etldatapipeline.commonutil.kafka.KafkaTopicCreator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class KafkaTopicInitializer {

    private final KafkaTopicCreator kafkaTopicCreator;

    @Autowired
    public KafkaTopicInitializer(KafkaTopicCreator kafkaTopicCreator) {
        this.kafkaTopicCreator = kafkaTopicCreator;
    }

    @PostConstruct
    public void initializeTopics() {
        kafkaTopicCreator.createTopics("investigation");
    }
}
