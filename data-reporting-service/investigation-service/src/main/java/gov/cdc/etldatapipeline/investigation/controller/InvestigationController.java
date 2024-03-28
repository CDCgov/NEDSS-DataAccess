package gov.cdc.etldatapipeline.investigation.controller;

import gov.cdc.etldatapipeline.investigation.service.DummyService;
import gov.cdc.etldatapipeline.investigation.service.InvestigationService;
import gov.cdc.etldatapipeline.investigation.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
public class InvestigationController {
    private final KafkaProducerService producerService;
    private final InvestigationService investigationService;
    private final String topicName = "cdc.nbs_odse.dbo.Investigation-dummy";

    private final DummyService dummyService;

    @PostMapping("/publish")
    public void publishMessageToKafka(@RequestBody String jsonData) {
        //producerService.sendMessage(topicName, jsonData);
//        System.err.println(investigationService.processInvestigation("263771897"));
        investigationService.processInvestigation("263771897");
    }


    @PostMapping("/publish-dummy")
    public void publishMessageToKafkaDummy(@RequestBody String jsonData) {
        dummyService.sendMessage(topicName, jsonData);
        //investigationService.processMessage();
    }
}
