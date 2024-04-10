package gov.cdc.etldatapipeline.person.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.config.KafkaConfig;
import gov.cdc.etldatapipeline.person.config.KafkaStreamsConfig;
import gov.cdc.etldatapipeline.person.service.PersonStatusService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class PersonServiceController {

    PersonStatusService dataPipelineStatusSvc;

    @Autowired
    private KafkaStreamsConfig kafkaStreamsConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    public PersonServiceController(PersonStatusService dataPipelineStatusSvc) {
        this.dataPipelineStatusSvc = dataPipelineStatusSvc;
    }


    @GetMapping("/status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth() {
        return this.dataPipelineStatusSvc.getHealthStatus();
    }

    @PostMapping(value = "/provider", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postProvider(@RequestBody String payLoad) {
        try {
            KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(
                    kafkaStreamsConfig.kStreamsConfigs().asProperties());
            producer.send(new ProducerRecord<>(kafkaConfig.getPersonTopicName(),
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
            producer.close();
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError().body("Failed to process the provider. Exception : " + ex.getMessage());
        }
    }

    @PostMapping(value = "/patient", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postPatient(@RequestBody String payLoad) {
        try {
            KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(
                    kafkaStreamsConfig.kStreamsConfigs().asProperties());
            producer.send(new ProducerRecord<>(kafkaConfig.getPersonTopicName(),
                    UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
            producer.close();
            return ResponseEntity.ok("Produced : " + payLoad);
        } catch (Exception ex) {
            return ResponseEntity.internalServerError().body("Failed to process the Patient. Exception : " + ex.getMessage());
        }
    }

}
