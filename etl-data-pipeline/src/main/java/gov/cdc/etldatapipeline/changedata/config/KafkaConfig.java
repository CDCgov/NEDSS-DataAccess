package gov.cdc.etldatapipeline.changedata.config;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Getter
public class KafkaConfig {
    @Value("${spring.kafka.stream.input.provider.output-topic-name}")
    private String providerAggregateTopicName;
    @Value("${spring.kafka.stream.input.person.topic-name}")
    private String personTopicName;
    @Value("${spring.kafka.stream.input.organization.topic-name}")
    private String organizationTopicName;
    @Value("${spring.kafka.stream.input.organization.output-topic-name}")
    private String organizationAggregateTopicName;

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
}


