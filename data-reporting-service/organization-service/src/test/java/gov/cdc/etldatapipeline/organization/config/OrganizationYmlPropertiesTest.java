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

        Assertions.assertEquals("nbs_Default", kafkaConfig.getDefaultDataTopic());
        Assertions.assertEquals("nbs_Organization", kafkaConfig.getOrganizationTopic());
        Assertions.assertEquals("elastic_search_organization", kafkaConfig.getOrganizationElasticSearchTopic());
        Assertions.assertEquals("nrt_organization", kafkaConfig.getOrganizationReportingTopic());

    }

}
