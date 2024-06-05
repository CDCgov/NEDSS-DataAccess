package gov.cdc.etldatapipeline.investigation.config;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConfigurationProperties
@Getter
public class KafkaConfig {

    @Value("${spring.kafka.stream.input.investigation.topic-name}")
    public String investigationInputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-reporting}")
    public String investigationReportingOutputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-confirmation}")
    public String investigationConfirmationOutputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-observation}")
    public String investigationObservationOutputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-notifications}")
    public String notificationsOutputTopicName;

    @Bean
    public NewTopic createInvestigationInputTopic() {
        return TopicBuilder.name(investigationInputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationTransformedOutputTopic() {
        return TopicBuilder.name(investigationReportingOutputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationConfirmationOutputTopic() {
        return TopicBuilder.name(investigationConfirmationOutputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationObservationOutputTopic() {
        return TopicBuilder.name(investigationObservationOutputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationNotificationsOutputTopic() {
        return TopicBuilder.name(notificationsOutputTopicName).build();
    }
}