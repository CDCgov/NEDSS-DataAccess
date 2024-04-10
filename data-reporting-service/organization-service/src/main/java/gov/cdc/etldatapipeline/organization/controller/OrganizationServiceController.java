package gov.cdc.etldatapipeline.organization.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.organization.config.KafkaConfig;
import gov.cdc.etldatapipeline.organization.config.KafkaStreamsConfig;
import gov.cdc.etldatapipeline.organization.service.OrganizationStatusService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class OrganizationServiceController {

    private final OrganizationStatusService organizationStatusService;

    @Autowired
    private KafkaStreamsConfig kafkaStreamsConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    public OrganizationServiceController(OrganizationStatusService organizationStatusSvc) {
        this.organizationStatusService = organizationStatusSvc;
    }


    @GetMapping("/status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth() {
        return this.organizationStatusService.getHealthStatus();
    }


    @PostMapping(value = "/organization", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postOrganization(@RequestBody String payLoad) {
        try {
            KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(
                    kafkaStreamsConfig.kStreamsConfigs().asProperties());
            producer.send(new ProducerRecord<>(kafkaConfig.getOrganizationTopic(),
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
            producer.close();
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError()
                    .body("Error processing the Organization data. Exception: " + ex.getMessage());
        }
    }

}
