package gov.cdc.etldatapipeline.investigation.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.stream.output.investigation.topic-name-reporting}")
    public String investigationReportingOutputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-confirmation}")
    public String investigationConfirmationOutputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-notification}")
    public String investigationNotificationOutputTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-observation}")
    public String investigationObservationOutputTopicName;

    @Bean
    public NewTopic createInvestigationTransformedOutputTopic() {
        return TopicBuilder.name(investigationReportingOutputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationConfirmationOutputTopic() {
        return TopicBuilder.name(investigationConfirmationOutputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationNotificationOutputTopic() {
        return TopicBuilder.name(investigationNotificationOutputTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationObservationOutputTopic() {
        return TopicBuilder.name(investigationObservationOutputTopicName).build();
    }
}
