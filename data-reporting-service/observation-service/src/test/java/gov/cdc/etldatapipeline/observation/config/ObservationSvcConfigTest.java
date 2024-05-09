package gov.cdc.etldatapipeline.observation.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(initializers = ConfigDataApplicationContextInitializer.class)
@EnableConfigurationProperties(value = KafkaConfig.class)
@ActiveProfiles("test")
public class ObservationSvcConfigTest {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Test
    void testBindingYMLConfigFile_SetsAllFields() {
        Assertions.assertEquals("nbs_Observation", kafkaConfig.getObservationTopicName());
        Assertions.assertEquals("nrt_observation", kafkaConfig.getObservationReportingOutputTopicName());
        Assertions.assertEquals("elastic_search_observation", kafkaConfig.getObservationElasticSearchOutputTopicName());
    }

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaConfig kafkaConfig() {
            return new KafkaConfig();
        }
    }
}
