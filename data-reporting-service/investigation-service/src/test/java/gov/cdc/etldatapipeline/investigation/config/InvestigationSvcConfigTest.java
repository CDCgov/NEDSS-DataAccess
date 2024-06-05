package gov.cdc.etldatapipeline.investigation.config;

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
public class InvestigationSvcConfigTest {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Test
    void testBindingYMLConfigFile_SetsAllFields() {
        Assertions.assertEquals("nbs_Public_health_case", kafkaConfig.getInvestigationInputTopicName());
        Assertions.assertEquals("nrt_investigation", kafkaConfig.getInvestigationReportingOutputTopicName());
        Assertions.assertEquals("nrt_investigation_confirmation", kafkaConfig.getInvestigationConfirmationOutputTopicName());
        Assertions.assertEquals("nrt_investigation_observation", kafkaConfig.getInvestigationObservationOutputTopicName());
        Assertions.assertEquals("nrt_notifications", kafkaConfig.getNotificationsOutputTopicName());
    }

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaConfig kafkaConfig() {
            return new KafkaConfig();
        }
    }
}
