package gov.cdc.etldatapipeline.organization.config;

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
    @Value("${spring.kafka.stream.input.organization.topic-name}")
    private String organizationTopic;
    @Value("${spring.kafka.stream.input.defaultData.topic-name}")
    private String defaultDataTopic;
    @Value("${spring.kafka.stream.output.organizationElastic.topic-name}")
    private String organizationElasticSearchTopic;
    @Value("${spring.kafka.stream.output.organizationReporting.topic-name}")
    private String organizationReportingTopic;


    @Bean
    public NewTopic createIncomingOrganizationTopicName() {
        log.info("Creating topic : " + organizationTopic);
        return TopicBuilder.name(organizationTopic).build();
    }

    @Bean
    public NewTopic createOrganizationReportingTopic() {
        log.info("Creating topic : " + organizationReportingTopic);
        return TopicBuilder.name(organizationReportingTopic).build();
    }

    @Bean
    public NewTopic createOrganizationElasticTopic() {
        log.info("Creating topic : " + organizationElasticSearchTopic);
        return TopicBuilder.name(organizationElasticSearchTopic).build();
    }
}


