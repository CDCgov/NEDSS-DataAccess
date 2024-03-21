package gov.cdc.etldatapipeline.person.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
@Configuration
@ConfigurationProperties
@Getter
public class KafkaConfig {
    @Value("${spring.kafka.stream.input.person.topic-name}")
    private String personTopicName;
    @Value("${spring.kafka.stream.input.provider.topic-name}")
    private String providerInputTopicName;
    @Value("${spring.kafka.stream.input.patient.topic-name}")
    private String patientInputTopicName;
    @Value("${spring.kafka.stream.input.organization.topic-name}")
    private String organizationTopicName;
    @Value("${spring.kafka.stream.input.defaultData.topic-name}")
    private String defaultDataTopicName;
    @Value("${spring.kafka.stream.output.provider.topic-name}")
    private String providerAggregateTopicName;
    @Value("${spring.kafka.stream.output.patient.topic-name}")
    private String patientAggregateTopicName;
    @Value("${spring.kafka.stream.output.organization.topic-name}")
    private String organizationAggregateTopicName;

    @Bean
    public NewTopic createPersonTopicName() {
        log.info("Creating topic : " + personTopicName);
        return TopicBuilder.name(personTopicName).build();
    }

    @Bean
    public NewTopic createInputProviderTopicName() {
        log.info("Creating topic : " + providerInputTopicName);
        return TopicBuilder.name(providerInputTopicName).build();
    }

    @Bean
    public NewTopic createAggregateProviderTopicName() {
        log.info("Creating topic : " + providerAggregateTopicName);
        return TopicBuilder.name(providerAggregateTopicName).build();
    }

    @Bean
    public NewTopic createOrganizationTopicName() {
        log.info("Creating topic : " + organizationTopicName);
        return TopicBuilder.name(organizationTopicName).build();
    }

    @Bean
    public NewTopic createAggregateOrganizationTopicName() {
        log.info("Creating topic : " + organizationAggregateTopicName);
        return TopicBuilder.name(organizationAggregateTopicName).build();
    }

    @Bean
    public NewTopic createInputPatientTopicName() {
        log.info("Creating topic : " + patientInputTopicName);
        return TopicBuilder.name(patientInputTopicName).build();
    }

    @Bean
    public NewTopic createAggregatePatientTopicName() {
        log.info("Creating topic : " + patientAggregateTopicName);
        return TopicBuilder.name(patientAggregateTopicName).build();
    }
}


