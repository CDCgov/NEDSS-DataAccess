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
    @Value("${spring.kafka.stream.input.ct-contact.topic-name}")
    private String ctContactTopicName;

    @Bean
    public NewTopic createNbsPagesTopicName() {
        return TopicBuilder.name(nbsPagesTopicName).build();
    }

    @Bean
    public String getNbsPagesTopicName() {
        return nbsPagesTopicName;
    }

    @Bean
    public String getPersonTopicName() {
        return personTopicName;
    }

    @Bean
    public String getParticipationTopicName() {
        return participationTopicName;
    }

    @Bean
    public String getCtContactTopicName() {
        return ctContactTopicName;
    }
}
