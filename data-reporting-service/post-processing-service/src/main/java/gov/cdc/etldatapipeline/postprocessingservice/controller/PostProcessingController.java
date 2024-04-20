package gov.cdc.etldatapipeline.postprocessingservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.postprocessingservice.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;


@RestController
@RequiredArgsConstructor
public class PostProcessingController {
    private final KafkaProducerService producerService;
    private final String investigationTopicName = "nbs_nrt_investigation";

    private final String observationTopicName = "nbs_nrt_observation";

    private final String patientTopicName = "nbs_nrt_patient";

    @PostMapping("/publish/inv")
    public void publishMessageToKafkaInv(@RequestBody Map<String, Object> data) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String key = objectMapper.writeValueAsString(data.get("key"));
        String value = objectMapper.writeValueAsString(data.get("value"));
        producerService.sendMessage(investigationTopicName, key, value);
    }

    @PostMapping("/publish/obs")
    public void publishMessageToKafkaObs(@RequestBody Map<String, Object> data) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String key = objectMapper.writeValueAsString(data.get("key"));
        String value = objectMapper.writeValueAsString(data.get("value"));
        producerService.sendMessage(observationTopicName, key, value);
    }

    @PostMapping("/publish/pat")
    public void publishMessageToKafkaPat(@RequestBody Map<String, Object> data) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String key = objectMapper.writeValueAsString(data.get("key"));
        String value = objectMapper.writeValueAsString(data.get("value"));
        producerService.sendMessage(patientTopicName, key, value);
    }
}
