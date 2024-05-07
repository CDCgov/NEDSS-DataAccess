package gov.cdc.etldatapipeline.organization.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.organization.config.KafkaConfig;
import gov.cdc.etldatapipeline.organization.service.OrganizationStatusService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class OrganizationServiceController {

    private final OrganizationStatusService organizationStatusService;

    private final KafkaConfig kafkaConfig;

    private final Producer<String, JsonNode> producer;

    @GetMapping("/reporting/organization-svc/status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth() {
        return this.organizationStatusService.getHealthStatus();
    }


    @PostMapping(value = "/reporting/organization-svc/produce", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postOrganization(@RequestBody String payLoad) {
        try {
            producer.send(new ProducerRecord<>(kafkaConfig.getOrganizationTopic(),
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError()
                    .body("Error processing the Organization data. Exception: " + ex.getMessage());
        }
    }

}
