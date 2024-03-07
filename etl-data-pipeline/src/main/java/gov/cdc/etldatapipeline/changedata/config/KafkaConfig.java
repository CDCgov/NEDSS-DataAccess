package gov.cdc.etldatapipeline.changedata.config;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Getter
public class
KafkaConfig {
    @Value("${spring.kafka.stream.input.nbs-pages.topic-name}")
    private String nbsPagesTopicName;
    @Value("${spring.kafka.stream.input.provider.topic-name}")
    private String providerTopicName;
    @Value("${spring.kafka.stream.input.provider.output-topic-name}")
    private String providerAggregateTopicName;
    @Value("${spring.kafka.stream.input.person.topic-name}")
    private String personTopicName;
    @Value("${spring.kafka.stream.input.participation.topic-name}")
    private String participationTopicName;
    @Value("${spring.kafka.stream.input.ct-contact.topic-name}")
    private String ctContactTopicName;
    @Value("${spring.kafka.stream.input.organization.topic-name}")
    private String organizationTopicName;
    @Value("${spring.kafka.stream.input.organization.output-topic-name}")
    private String organizationAggregateTopicName;
    @Value("${spring.kafka.stream.input.patient.output-topic-name}")
    private String patientAggregateTopicName;

    @Bean
    public NewTopic createNbsPagesTopicName() {
        return TopicBuilder.name(nbsPagesTopicName).build();
    }

    @Bean
    public NewTopic createPersonTopicName() {
        return TopicBuilder.name(personTopicName).build();
    }

    @Bean
    public NewTopic createAggregateProviderTopicName() {
        return TopicBuilder.name(providerAggregateTopicName).build();
    }

    @Bean
    public NewTopic createOrganizationTopicName() {
        return TopicBuilder.name(organizationTopicName).build();
    }

    @Bean
    public NewTopic createAggregateOrganizationTopicName() {
        return TopicBuilder.name(organizationAggregateTopicName).build();
    }

    @Bean
    public NewTopic createAggregatePatientTopicName() {
        return TopicBuilder.name(patientAggregateTopicName).build();
    }


}


