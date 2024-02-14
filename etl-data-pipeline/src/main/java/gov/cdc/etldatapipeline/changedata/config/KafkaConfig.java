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

    @Bean
    public NewTopic createNbsPagesTopicName() {
        return TopicBuilder.name(nbsPagesTopicName).build();
    }

    @Bean
    public NewTopic createProviderTopicName() {
        return TopicBuilder.name(providerTopicName).build();
    }

    @Bean
    public NewTopic createAggregateProviderTopicName() {
        return TopicBuilder.name(providerAggregateTopicName).build();
    }
}
