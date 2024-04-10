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
    @Value("${spring.kafka.stream.input.defaultData.topic-name}")
    private String defaultDataTopicName;
    @Value("${spring.kafka.stream.output.providerElastic.topic-name}")
    private String providerElasticSearchTopic;
    @Value("${spring.kafka.stream.output.providerReporting.topic-name}")
    private String providerReportingTopic;
    @Value("${spring.kafka.stream.output.patientElastic.topic-name}")
    private String patientElasticSearchTopic;
    @Value("${spring.kafka.stream.output.patientReporting.topic-name}")
    private String patientReportingTopic;

    @Bean
    public NewTopic createPersonTopicName() {
        log.info("Creating topic : " + personTopicName);
        return TopicBuilder.name(personTopicName).build();
    }

    @Bean
    public NewTopic createPatientElasticTopicName() {
        log.info("Creating topic : " + patientElasticSearchTopic);
        return TopicBuilder.name(patientElasticSearchTopic).build();
    }

    @Bean
    public NewTopic createPatientReportingTopicName() {
        log.info("Creating topic : " + patientReportingTopic);
        return TopicBuilder.name(patientReportingTopic).build();
    }

    @Bean
    public NewTopic createProviderElasticSearchTopicName() {
        log.info("Creating topic : " + providerElasticSearchTopic);
        return TopicBuilder.name(providerElasticSearchTopic).build();
    }

    @Bean
    public NewTopic createProviderReportingTopicName() {
        log.info("Creating topic : " + providerReportingTopic);
        return TopicBuilder.name(providerReportingTopic).build();
    }
}


