package gov.cdc.etldatapipeline.person.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.config.KafkaConfig;
import gov.cdc.etldatapipeline.person.service.PersonStatusService;
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

public class PersonServiceControllerTests {

    @Mock
    private PersonStatusService dataPipelineStatusService;

    @Mock
    private KafkaConfig kafkaConfig;

    private MockProducer<String, JsonNode> mockProducer;

    @InjectMocks
    private PersonServiceController controller;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockProducer = new MockProducer<>(true, new StringSerializer(), new JsonSerializer<>());
        controller = new PersonServiceController(dataPipelineStatusService, kafkaConfig, mockProducer);
    }

    @Test
    public void testPostProvider() throws Exception {
        String payload = "{\"payload\": {\"after\": {\"cd\": \"PRV\"}}}";
        JsonNode jsonNode = new ObjectMapper().readTree(payload);
        when(kafkaConfig.getPersonTopicName()).thenReturn("person-topic");

        ResponseEntity<String> response = controller.postProvider(payload);

        assertEquals("Produced : " + payload, response.getBody());
        assertEquals(HttpStatus.OK, response.getStatusCode());

        assertEquals(mockProducer.history().size(), 1);
        List<ProducerRecord<String, JsonNode>> sentRecordList = mockProducer.history();
        ProducerRecord<String, JsonNode> sentRecord = sentRecordList.get(sentRecordList.size() - 1);
        assertEquals("person-topic", sentRecord.topic());
        assertEquals(jsonNode, sentRecord.value());
        assertNotNull(sentRecord.key());
    }

    @Test
    public void testPostPatient() throws Exception {
        String payload = "{\"payload\": {\"after\": {\"cd\": \"PAT\"}}}";
        JsonNode jsonNode = new ObjectMapper().readTree(payload);
        when(kafkaConfig.getPersonTopicName()).thenReturn("person-topic");

        ResponseEntity<String> response = controller.postPatient(payload);

        assertEquals("Produced : " + payload, response.getBody());
        assertEquals(HttpStatus.OK, response.getStatusCode());

        assertEquals(mockProducer.history().size(), 1);
        List<ProducerRecord<String, JsonNode>> sentRecordList = mockProducer.history();
        ProducerRecord<String, JsonNode> sentRecord = sentRecordList.get(sentRecordList.size() - 1);
        assertEquals("person-topic", sentRecord.topic());
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
