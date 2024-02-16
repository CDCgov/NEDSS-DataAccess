package gov.cdc.etldatapipeline.changedata.controller;

import gov.cdc.etldatapipeline.changedata.config.KafkaStreamsConfig;
import gov.cdc.etldatapipeline.changedata.service.DataPipelineStatusService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
public class DataPipelineController {
    private static final Logger LOG = LoggerFactory.getLogger(DataPipelineController.class);

    DataPipelineStatusService dataPipelineStatusSvc;

    @Autowired
    private KafkaStreamsConfig kafkaStreamsConfig;

    public DataPipelineController(DataPipelineStatusService dataPipelineStatusSvc) {
        this.dataPipelineStatusSvc = dataPipelineStatusSvc;
    }


    @GetMapping("/data-pipeline-status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth(){
       return this.dataPipelineStatusSvc.getHealthStatus();
    }

    @PostMapping(value = "/provider", consumes = "application/json", produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> postProvider(@RequestBody List<String> providerUids){
        KafkaProducer<String,List<String>> producer = new KafkaProducer<>(
                kafkaStreamsConfig.kStreamsConfigs().asProperties());
        producer.send(new ProducerRecord<>("cdc.nbs_odse.dbo.Provider",
                UUID.randomUUID().toString(), providerUids));
        producer.close();
        return ResponseEntity.ok("Produced : " + providerUids);
    }
}
