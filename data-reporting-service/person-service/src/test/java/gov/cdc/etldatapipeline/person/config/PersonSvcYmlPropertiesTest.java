package gov.cdc.etldatapipeline.person.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(initializers = ConfigDataApplicationContextInitializer.class)
@EnableConfigurationProperties(value = KafkaConfig.class)
@ActiveProfiles("test")
public class PersonSvcYmlPropertiesTest {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Test
    void whenBindingYMLConfigFile_thenAllFieldsAreSet() {
        Assertions.assertEquals("nbs_default", kafkaConfig.getDefaultDataTopicName());
        Assertions.assertEquals("nbs_Person", kafkaConfig.getPersonTopicName());
        Assertions.assertEquals("elastic_nrt_patient", kafkaConfig.getPatientElasticSearchTopic());
        Assertions.assertEquals("nrt_patient", kafkaConfig.getPatientReportingTopic());
        Assertions.assertEquals("elastic_nrt_provider", kafkaConfig.getProviderElasticSearchTopic());
        Assertions.assertEquals("nrt_provider", kafkaConfig.getProviderReportingTopic());
    }

}
