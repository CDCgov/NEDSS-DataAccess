package gov.cdc.etldatapipeline.organization.config;

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
public class OrganizationYmlPropertiesTest {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Test
    void whenBindingYMLConfigFile_thenAllFieldsAreSet() {
        Assertions.assertEquals("cdc.nbs_odse.dbo.Default.input", kafkaConfig.getDefaultDataTopic());
        Assertions.assertEquals("cdc.nbs_odse.dbo.Organization.input", kafkaConfig.getOrganizationTopic());
        Assertions.assertEquals("nbs_organization_elastic", kafkaConfig.getOrganizationElasticSearchTopic());
        Assertions.assertEquals("nbs_organization", kafkaConfig.getOrganizationReportingTopic());
    }

}
