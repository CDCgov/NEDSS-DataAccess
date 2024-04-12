package gov.cdc.etldatapipeline.observation.controller;

import gov.cdc.etldatapipeline.observation.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ObservationController {
    private final KafkaProducerService producerService;
    private final String topicName = "cdc.nbs_odse.dbo.Observation";

    @Autowired
    public ObservationController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/publish")
    public void publishMessageToKafka(@RequestBody String jsonData) {
        producerService.sendMessage(topicName, jsonData);
    }
}
