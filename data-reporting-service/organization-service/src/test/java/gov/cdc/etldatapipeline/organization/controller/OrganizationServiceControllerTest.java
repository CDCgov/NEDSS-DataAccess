package gov.cdc.etldatapipeline.organization.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.organization.config.KafkaConfig;
import gov.cdc.etldatapipeline.organization.service.OrganizationStatusService;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrganizationServiceControllerTest {

    @Mock
    private OrganizationStatusService dataPipelineStatusService;

    @Mock
    private KafkaConfig kafkaConfig;

    private MockProducer<String, JsonNode> mockProducer;

    @InjectMocks
    private OrganizationServiceController controller;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockProducer = new MockProducer<>(true, new StringSerializer(), new JsonSerializer<>());
        controller = new OrganizationServiceController(dataPipelineStatusService, kafkaConfig, mockProducer);
    }

    @Test
    public void testPostOrganization() throws Exception {
        String payload = "{\"payload\": {\"after\": {\"organization_uid\": \"10036000\"}}}";
        JsonNode jsonNode = new ObjectMapper().readTree(payload);
        when(kafkaConfig.getOrganizationTopic()).thenReturn("org-topic");

        ResponseEntity<String> response = controller.postOrganization(payload);

        assertEquals("Produced : " + payload, response.getBody());
        assertEquals(HttpStatus.OK, response.getStatusCode());

        assertEquals(mockProducer.history().size(), 1);
        List<ProducerRecord<String, JsonNode>> sentRecordList = mockProducer.history();
        ProducerRecord<String, JsonNode> sentRecord = sentRecordList.get(sentRecordList.size() - 1);
        assertEquals("org-topic", sentRecord.topic());
        assertEquals(jsonNode, sentRecord.value());
        assertNotNull(sentRecord.key());
    }

    @Test
    void testGetStatusHealth() throws Exception {
        final String responseBody = "Person Service Status OK";
        when(dataPipelineStatusService.getHealthStatus()).thenReturn(ResponseEntity.ok(responseBody));

        ResponseEntity<String> response = controller.getDataPipelineStatusHealth();

        verify(dataPipelineStatusService).getHealthStatus();
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(responseBody, response.getBody());
    }
}