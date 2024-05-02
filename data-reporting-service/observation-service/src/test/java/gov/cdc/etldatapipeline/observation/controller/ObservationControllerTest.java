package gov.cdc.etldatapipeline.observation.controller;

import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import gov.cdc.etldatapipeline.observation.service.KafkaProducerService;

public class ObservationControllerTest {

    private MockMvc mockMvc;

    @Mock
    private KafkaProducerService kafkaProducerService;

    @InjectMocks
    private ObservationController observationController;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(observationController).build();
    }

    @Test
    public void publishMessageToKafkaTest() throws Exception {
        String jsonData = "{\"key\":\"value\"}";

        mockMvc.perform(post("/publish")
                        .contentType("application/json")
                        .content(jsonData))
                .andExpect(status().isOk());

        verify(kafkaProducerService).sendMessage(eq("cdc.nbs_odse.dbo.Observation"), anyString());
    }
}
