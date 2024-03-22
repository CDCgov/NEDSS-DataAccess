package gov.cdc.etldatapipeline.investigation.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.stream.input.investigation.topic-name}")
    private String investigationTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name}")
    private String investigationAggregateTopicName;

    @Value("${spring.kafka.stream.output.investigation.topic-name-transformed}")
    public String investigationTransformedOutputTopicName = "cdc.nbs_odse.dbo.Investigation.output-transformed";

    @Bean
    public NewTopic createAggregateInvestigationTopicName() {
        return TopicBuilder.name(investigationAggregateTopicName).build();
    }

    @Bean
    public NewTopic createInvestigationTransformedOutputTopic() {
        return TopicBuilder.name(investigationTransformedOutputTopicName).build();
    }
}