package gov.cdc.etldatapipeline.observation.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.observation.TestUtils;
import gov.cdc.etldatapipeline.observation.repository.IObservationRepository;
import gov.cdc.etldatapipeline.observation.repository.model.dto.Observation;
import gov.cdc.etldatapipeline.observation.util.ProcessObservationDataUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ObservationServiceTest {

    @Mock
    private IObservationRepository observationRepository;

    @Mock KafkaTemplate<String, String> kafkaTemplate;

    @Captor
    private ArgumentCaptor<String> topicCaptor;

    @Captor
    private ArgumentCaptor<String> messageCaptor;


    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    ProcessObservationDataUtil transformer = new ProcessObservationDataUtil();

    @Test
    void testProcessMessage() {
        String observationTopic = "Observation";
        String observationTopicOutput = "ObservationOutput";
        String observationTopicOutputTransformed = "ObservationOutputTransformed";

        // Mocked input data
        Long observationUid = 123456789L;
        String obsDomainCdSt = "Order";
        String payload = "{\"payload\": {\"after\": {\"observation_uid\": \"" + observationUid + "\"}}}";

        Observation observation = constructObservation(observationUid, obsDomainCdSt);
        when(observationRepository.computeObservations(eq(String.valueOf(observationUid)))).thenReturn(Optional.of(observation));

        final String expectedTransformed = transformer.transformObservationData(observation).toString();

        validateData(observationTopic, observationTopicOutput,
                observationTopicOutputTransformed, payload, observationUid);

        verify(kafkaTemplate).send(topicCaptor.capture(), messageCaptor.capture());
        assertEquals(observationTopicOutputTransformed, topicCaptor.getValue());
        assertEquals(expectedTransformed, messageCaptor.getValue());
        verify(observationRepository).computeObservations(eq(String.valueOf(observationUid)));
    }

    private Observation constructObservation(Long observationUid, String obsDomainCdSt1) {
        String filePathPrefix = "rawDataFiles/";
        Observation observation = new Observation();
        observation.setId(observationUid);
        observation.setObsDomainCdSt1(obsDomainCdSt1);
        observation.setPersonParticipations(TestUtils.readFileData(filePathPrefix + "PersonParticipations.json"));
        return observation;
    }

    private void validateData(String inputTopicName, String outputTopicName, String transformedTopicName,
                              String payload, Long expectedUid) {
        StreamsBuilder builder = new StreamsBuilder();
        final var observationService = getObservationService(inputTopicName, outputTopicName, transformedTopicName);
        observationService.processMessage(builder);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), new Properties());
        final Serde<String> serdeString = Serdes.String();
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
                inputTopicName, serdeString.serializer(), serdeString.serializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(
                outputTopicName, serdeString.deserializer(), serdeString.deserializer());

        inputTopic.pipeInput("100000001", payload);
        KeyValue<String, String> result = outputTopic.readKeyValue();
        try {
            Observation actual = objectMapper.readValue(result.value, Observation.class);
            assertEquals(expectedUid, actual.getId());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        testDriver.close();
    }

    private ObservationService getObservationService(String inputTopicName, String outputTopicName, String transformedTopicName) {
        ObservationService observationService = new ObservationService(observationRepository, kafkaTemplate, new ProcessObservationDataUtil());
        observationService.setObservationTopic(inputTopicName);
        observationService.setObservationTopicOutputReporting(outputTopicName);
        observationService.setObservationTopicOutputElasticSearch(transformedTopicName);
        return observationService;
    }


}
