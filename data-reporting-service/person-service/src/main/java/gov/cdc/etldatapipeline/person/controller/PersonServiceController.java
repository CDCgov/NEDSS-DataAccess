package gov.cdc.etldatapipeline.person.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.config.KafkaConfig;
import gov.cdc.etldatapipeline.person.service.PersonStatusService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class PersonServiceController {

    private final PersonStatusService dataPipelineStatusSvc;

    private final KafkaConfig kafkaConfig;

    private final Producer<String, JsonNode> producer;

    @GetMapping("/reporting/person-svc/status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth() {
        return this.dataPipelineStatusSvc.getHealthStatus();
    }

    @PostMapping(value = "/reporting/person-svc/provider", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postProvider(@RequestBody String payLoad) {
        try {
            producer.send(new ProducerRecord<>(kafkaConfig.getPersonTopicName(),
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError().body("Failed to process the provider. Exception : " + ex.getMessage());
        }
    }

    @PostMapping(value = "/reporting/person-svc/patient", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postPatient(@RequestBody String payLoad) {
        try {
            producer.send(new ProducerRecord<>(kafkaConfig.getPersonTopicName(),
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError().body("Failed to process the Patient. Exception : " + ex.getMessage());
        }
    }

}
