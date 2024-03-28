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
        Assertions.assertEquals("cdc.nbs_odse.dbo.Default.input", kafkaConfig.getDefaultDataTopicName());
        Assertions.assertEquals("cdc.nbs_odse.dbo.Person", kafkaConfig.getPersonTopicName());
        Assertions.assertEquals("cdc.nbs_odse.dbo.Organization.input", kafkaConfig.getOrganizationTopicName());
        Assertions.assertEquals("cdc.nbs_odse.dbo.Patient.input", kafkaConfig.getPatientInputTopicName());
        Assertions.assertEquals("cdc.nbs_odse.dbo.Provider.input", kafkaConfig.getProviderInputTopicName());
        Assertions.assertEquals("nbs_patient_elastic", kafkaConfig.getPatientElasticSearchTopic());
        Assertions.assertEquals("nbs_patient", kafkaConfig.getPatientReportingTopic());
        Assertions.assertEquals("nbs_provider_elastic", kafkaConfig.getProviderElasticTopic());
        Assertions.assertEquals("nbs_provider", kafkaConfig.getProviderReportingTopic());
        Assertions.assertEquals("nbs_organization", kafkaConfig.getOrganizationAggregateTopicName());
    }

}
