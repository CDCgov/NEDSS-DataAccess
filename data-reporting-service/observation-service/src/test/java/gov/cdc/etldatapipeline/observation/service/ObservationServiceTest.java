//package gov.cdc.etldatapipeline.observation.service;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import gov.cdc.etldatapipeline.observation.repository.IObservationRepository;
//import gov.cdc.etldatapipeline.observation.repository.model.Observation;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.kstream.Consumed;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.Produced;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//import java.util.Optional;
//
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.*;
//
//class ObservationServiceTest {
//
//    @Mock
//    private IObservationRepository observationRepository;
//
//    @InjectMocks
//    private ObservationService observationService;
//
//    @BeforeEach
//    void setUp() {
//        MockitoAnnotations.openMocks(this);
//    }
//
//    @Test
//    void testProcessMessage() throws Exception {
//        // Mocked input data
//        String observationUid = "123456789";
//        String inputJson = "{\"payload\": {\"after\": {\"observation_uid\": \"" + observationUid + "\"}}}";
//
//        // Mocked observation
//        Observation observation = new Observation();
//        observation.setId(Long.valueOf(observationUid));
//
//        // Mocked ObjectMapper
//        ObjectMapper objectMapper = mock(ObjectMapper.class);
//        JsonNode jsonNode = objectMapper.readTree(inputJson);
//        when(objectMapper.readTree(any(String.class))).thenReturn(jsonNode);
//        when(objectMapper.writeValueAsString(any())).thenReturn("mockedObservationJson");
//
//        // Mocked observationRepository behavior
//        when(observationRepository.computeObservations(eq(observationUid))).thenReturn(Optional.of(observation));
//
//        StreamsBuilder streamsBuilder = mock(StreamsBuilder.class);
//        KStream<String, String> mockKStream = mock(KStream.class);
//
//        // Use thenAnswer() instead of thenReturn()
//        when(streamsBuilder.stream(anyString(), any())).thenAnswer(invocation -> {
//            String topicName = invocation.getArgument(0);
//            Consumed<String, String> consumed = invocation.getArgument(1);
//            // Return the mocked KStream
//            return mockKStream;
//        });
//        // Mocked Consumed and Produced
//        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
//        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
//
//        // Test the method
//        observationService.processMessage(streamsBuilder);
//
//        // Verify that expected methods were called
//        verify(streamsBuilder).stream(eq(observationService.observationTopicOutput), eq(consumed));
//        verify(observationRepository).computeObservations(eq(observationUid));
//    }
//}
