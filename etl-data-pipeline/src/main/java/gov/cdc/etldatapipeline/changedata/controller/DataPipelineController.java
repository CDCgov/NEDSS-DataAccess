package gov.cdc.etldatapipeline.changedata.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.changedata.config.KafkaConfig;
import gov.cdc.etldatapipeline.changedata.config.KafkaStreamsConfig;
import gov.cdc.etldatapipeline.changedata.service.DataPipelineStatusService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class DataPipelineController {

    DataPipelineStatusService dataPipelineStatusSvc;

    @Autowired
    private KafkaStreamsConfig kafkaStreamsConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    public DataPipelineController(DataPipelineStatusService dataPipelineStatusSvc) {
        this.dataPipelineStatusSvc = dataPipelineStatusSvc;
    }


    @GetMapping("/data-pipeline-status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth() {
        return this.dataPipelineStatusSvc.getHealthStatus();
    }

    @PostMapping(value = "/provider", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postProvider(@RequestBody String payLoad) throws JsonProcessingException {
        KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(
                kafkaStreamsConfig.kStreamsConfigs().asProperties());
        producer.send(new ProducerRecord<>(kafkaConfig.getPersonTopicName(),
                UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
        producer.close();
        return ResponseEntity.ok("Produced : " + payLoad);
    }

    @PostMapping(value = "/organization", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postOrganization(@RequestBody String payLoad) throws JsonProcessingException {
        KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(
                kafkaStreamsConfig.kStreamsConfigs().asProperties());
        producer.send(new ProducerRecord<>(kafkaConfig.getOrganizationTopicName(),
                UUID.randomUUID().toString(), new ObjectMapper().readTree(payLoad)));
        producer.close();
        return ResponseEntity.ok("Produced : " + payLoad);
    }

}
