package gov.cdc.etldatapipeline.changedata.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.stream.input.nbs-pages.topic-name}")
    private String nbsPagesTopicName;

    @Value("${spring.kafka.stream.input.person.topic-name}")
    private String personTopicName;

    @Value("${spring.kafka.stream.input.participation.topic-name}")
    private String participationTopicName;

    @Bean
    public String nbsPagesTopicName() {
        return nbsPagesTopicName;
    }

    @Bean
    public String personTopicName() {
        return personTopicName;
    }

    @Bean
    public String participationTopicName() {
        return participationTopicName;
    }

    @Bean
    public NewTopic createNbsPagesTopicName() {
        return TopicBuilder.name(nbsPagesTopicName).build();
    }

}
