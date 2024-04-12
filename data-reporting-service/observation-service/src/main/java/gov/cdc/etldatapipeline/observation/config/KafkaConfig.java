package gov.cdc.etldatapipeline.observation.config;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Getter
public class KafkaConfig {
    @Value("${spring.kafka.stream.input.observation.topic-name}")
    private String observationTopicName;

    @Value("${spring.kafka.stream.output.observation.topic-name}")
    private String observationAggregateTopicName;

    @Value("${spring.kafka.stream.output.observation.topic-name-transformed}")
    public String observationTransformedOutputTopicName = "cdc.nbs_odse.dbo.Observation.output-transformed";

    @Bean
    public NewTopic createAggregateObservationTopicName() {
        return TopicBuilder.name(observationAggregateTopicName).build();
    }

    @Bean
    public NewTopic createObservationTransformedOutputTopic() {
        return TopicBuilder.name(observationTransformedOutputTopicName).build();
    }

}


