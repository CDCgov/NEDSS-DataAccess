package gov.cdc.etldatapipeline.organization;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.commonutil.model.avro.DataEnvelope;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationSp;
import gov.cdc.etldatapipeline.organization.repository.OrgRepository;
import gov.cdc.etldatapipeline.organization.service.OrganizationService;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationTransformers;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Set;

import static gov.cdc.etldatapipeline.commonutil.TestUtils.readFileData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
public class OrganizationServiceTest {

    @Mock
    private OrgRepository orgRepository;

    @Mock
    private OrganizationTransformers transformer;

    @Mock
    private KafkaTemplate<String, DataEnvelope> kafkaTemplate;

    private OrganizationService organizationService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String orgTopic = "OrgTopic";
    private final String orgElasticTopic = "OrgElasticTopic";
    private final String orgReportingTopic = "OrgReportingTopic";


    @BeforeEach
    public void setUp() {
        organizationService = new OrganizationService(orgRepository, transformer);
        organizationService.setKafkaTemplate(kafkaTemplate);
    }

    @Test
    public void testOrgReportingData() throws Exception {
        OrganizationSp orgSp = objectMapper.readValue(readFileData("orgcdc/orgSp.json"), OrganizationSp.class);
        Mockito.when(orgRepository.computeAllOrganizations(anyString())).thenReturn(Set.of(orgSp));

        // Validate Patient Reporting Data Transformation
//        validateDataTransformation(
//                readFileData("orgcdc/OrgChangeData.json"),
//                orgTopic,
//                orgReportingTopic,
//                "orgtransformed/OrgReporting.json",
//                "orgtransformed/OrgKey.json");

        DataEnvelope reportingData = new DataEnvelope();
        Mockito.when(transformer.processData(orgSp, OrganizationType.ORGANIZATION_REPORTING)).thenReturn(reportingData);

        validateDataTransformation("orgcdc/OrgChangeData.json", orgReportingTopic, reportingData);
    }

//    @Test
//    public void testOrgElasticData() throws JsonProcessingException {
//        OrganizationSp orgSp = objectMapper.readValue(readFileData("orgcdc/orgSp.json"), OrganizationSp.class);
//        Mockito.when(orgRepository.computeAllOrganizations(anyString())).thenReturn(Set.of(orgSp));
//
//        // Validate Patient Reporting Data Transformation
//        validateDataTransformation(
//                readFileData("orgcdc/OrgChangeData.json"),
//                orgTopic,
//                orgElasticTopic,
//                "orgtransformed/OrgElastic.json",
//                "orgtransformed/OrgKey.json");
//    }

    @Test
    public void testOrgElasticData() throws Exception {
        OrganizationSp orgSp = objectMapper.readValue(readFileData("orgcdc/orgSp.json"), OrganizationSp.class);
        Mockito.when(orgRepository.computeAllOrganizations(anyString())).thenReturn(Set.of(orgSp));

        DataEnvelope elasticData = new DataEnvelope();
        Mockito.when(transformer.processData(orgSp, OrganizationType.ORGANIZATION_ELASTIC_SEARCH)).thenReturn(elasticData);

        validateDataTransformation("orgcdc/OrgChangeData.json", orgElasticTopic, elasticData);
    }

    private void validateDataTransformation(String changeDataFilePath, String expectedTopic, DataEnvelope expectedData) throws Exception {
        String changeData = readFileData(changeDataFilePath);

        //JsonNode payloadNode = objectMapper.readTree(changeData).at("/payload/after");

        //Organization organization = objectMapper.treeToValue(payloadNode, Organization.class);
        //String message = objectMapper.writeValueAsString(organization);

//        // Spy on UtilHelper to mock deserializePayload method
//        UtilHelper utilHelperSpy = Mockito.spy(UtilHelper.getInstance());
//        Mockito.doReturn(organization).when(utilHelperSpy).deserializePayload(anyString(), anyString(), Mockito.eq(Organization.class));
//
//        // Inject the spy into the OrganizationService
//        UtilHelper.setInstance(utilHelperSpy);

        organizationService.processMessage(changeData, expectedTopic);

        ArgumentCaptor<DataEnvelope> dataCaptor = ArgumentCaptor.forClass(DataEnvelope.class);
        Mockito.verify(kafkaTemplate, Mockito.times(1)).send(Mockito.eq(expectedTopic), dataCaptor.capture());

        DataEnvelope actualData = dataCaptor.getValue();
        assertEquals(expectedData, actualData);
    }

}
