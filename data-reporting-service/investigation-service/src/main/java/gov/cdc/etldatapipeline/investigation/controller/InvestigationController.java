package gov.cdc.etldatapipeline.investigation.controller;

import gov.cdc.etldatapipeline.investigation.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class InvestigationController {
    private final KafkaProducerService producerService;
    private final String topicName = "cdc.nbs_odse.dbo.Investigation";

    @Autowired
    public InvestigationController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/publish")
    public void publishMessageToKafka(@RequestBody String jsonData) {
        producerService.sendMessage(topicName, jsonData);
    }
}
