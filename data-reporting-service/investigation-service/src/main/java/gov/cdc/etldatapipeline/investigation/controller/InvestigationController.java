package gov.cdc.etldatapipeline.investigation.controller;

import gov.cdc.etldatapipeline.investigation.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
public class InvestigationController {

    private final KafkaProducerService producerService;
    private final String topicName = "cdc.nbs_odse.dbo.Investigation";

    @PostMapping("/publish")
    public void publishMessageToKafka(@RequestBody String jsonData) {
        producerService.sendMessage(topicName, jsonData);
    }
}
