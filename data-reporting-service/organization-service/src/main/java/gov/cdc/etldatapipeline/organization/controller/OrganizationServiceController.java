package gov.cdc.etldatapipeline.organization.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.organization.service.OrganizationStatusService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class OrganizationServiceController {

    private final OrganizationStatusService organizationStatusService;

    private final KafkaTemplate<String, JsonNode> jsonNodeKafkaTemplate;

    @Value("${spring.kafka.input.topic-name}")
    private String orgTopicName;

    @GetMapping("/reporting/organization-svc/status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth() {
        return this.organizationStatusService.getHealthStatus();
    }


    @PostMapping(value = "/reporting/organization-svc/produce", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postOrganization(@RequestBody String payLoad) {
        try {
            jsonNodeKafkaTemplate.send(orgTopicName,
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad));
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError()
                    .body("Error processing the Organization data. Exception: " + ex.getMessage());
        }
    }

}
