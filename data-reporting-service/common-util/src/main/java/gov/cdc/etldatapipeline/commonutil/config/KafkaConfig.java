package gov.cdc.etldatapipeline.commonutil.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.stream.output.investigation.topic-name}")
    private String investigationAggregateTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-transformed}")
    public String investigationTransformedOutputTopicName = "cdc.nbs_odse.dbo.Investigation.output-transformed";

    @Value("${spring.kafka.stream.output.investigation.topic-name-confirmation}")
    public String investigationConfirmationOutputTopicName = "cdc.nbs_odse.dbo.Investigation.Confirmation";

    @Value("${spring.kafka.stream.output.investigation.topic-name-notification}")
    public String investigationNotificationOutputTopicName = "cdc.nbs_odse.dbo.Investigation.Notification";

    @Value("${spring.kafka.stream.output.investigation.topic-name-observation}")
    public String investigationObservationOutputTopicName = "cdc.nbs_odse.dbo.Investigation.output.Observation";

    @Bean
    public NewTopic createAggregateInvestigationTopicName() {
        return TopicBuilder.name(investigationAggregateTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationTransformedOutputTopic() {
        return TopicBuilder.name(investigationTransformedOutputTopicName).build();
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

    @Bean
    public NewTopic createInvestigationTopic() {
        return TopicBuilder.name("cdc.nbs_odse.dbo.Investigation").build();
    }
}
