package gov.cdc.etldatapipeline.organization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.commonutil.model.avro.DataEnvelope;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationSp;
import gov.cdc.etldatapipeline.organization.repository.OrgRepository;
import gov.cdc.etldatapipeline.organization.service.OrganizationService;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationTransformers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static gov.cdc.etldatapipeline.commonutil.TestUtils.readFileData;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
public class OrganizationServiceTest {

    @Mock
    OrgRepository orgRepository;

    private final String orgTopic = "OrgTopic";
    private final String orgElasticTopic = "OrgElasticTopic";
    private final String orgReportingTopic = "OrgReportingTopic";
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void testOrgReportingData() throws JsonProcessingException {
        OrganizationSp orgSp = objectMapper.readValue(readFileData("orgcdc/orgSp.json"), OrganizationSp.class);
        Mockito.when(orgRepository.computeAllOrganizations(anyString())).thenReturn(Set.of(orgSp));

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("orgcdc/OrgChangeData.json"),
                orgTopic,
                orgReportingTopic,
                "orgtransformed/OrgReporting.json",
                "orgtransformed/OrgKey.json");
    }

    @Test
    public void testOrgElasticData() throws JsonProcessingException {
        OrganizationSp orgSp = objectMapper.readValue(readFileData("orgcdc/orgSp.json"), OrganizationSp.class);
        Mockito.when(orgRepository.computeAllOrganizations(anyString())).thenReturn(Set.of(orgSp));

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("orgcdc/OrgChangeData.json"),
                orgTopic,
                orgElasticTopic,
                "orgtransformed/OrgElastic.json",
                "orgtransformed/OrgKey.json");
    }

    /**
     * Create a mock Kafka cluster and do stream processing of the Patient/Provider data
     *
     * @param incomingChangeData    Debezium Change Data
     * @param inputTopicName        Input Topic to monitor
     * @param outputTopicName       Output Topic to produce the transformed data
     * @param expectedValueFilePath Expected transformed Json Value Data in the DataEnvelope format
     * @param expectedKeyFilePath   Expected transformed Json Key Data in the DataEnvelope format
     */
    private void validateDataTransformation(
            String incomingChangeData,
            String inputTopicName,
            String outputTopicName,
            String expectedValueFilePath,
            String expectedKeyFilePath) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        OrganizationService ks = getKafkaStreamService();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic(outputTopicName, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", incomingChangeData);
            List<KeyValue<String, String>> actualData = outputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Provider Payload
            TypeReference<DataEnvelope> dataEnvelopeTypeReference = new TypeReference<>() {
            };

            DataEnvelope<DataEnvelope> actualKey
                    = objectMapper.readValue(actualData.get(0).key, dataEnvelopeTypeReference);
            Assertions.assertEquals(
                    objectMapper.readValue(readFileData(expectedKeyFilePath), dataEnvelopeTypeReference),
                    actualKey);

            //Validate the Patient Key
            DataEnvelope<DataEnvelope> actualValue
                    = objectMapper.readValue(actualData.get(0).value, dataEnvelopeTypeReference);
            //Construct expected Patient Key
            Assertions.assertEquals(
                    objectMapper.readValue(readFileData(expectedValueFilePath), dataEnvelopeTypeReference),
                    actualValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private OrganizationService getKafkaStreamService() {
        OrganizationService ks = new OrganizationService(orgRepository, new OrganizationTransformers());
        ks.setOrgTopicName(orgTopic);
        ks.setOrgElasticSearchTopic(orgElasticTopic);
        ks.setOrgReportingOutputTopic(orgReportingTopic);
        return ks;
    }

}
