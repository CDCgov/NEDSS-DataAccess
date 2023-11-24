package gov.cdc.etldatapipeline.changedata.controller;

import gov.cdc.etldatapipeline.changedata.service.DataPipelineStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataPipelineController {
    private static final Logger LOG = LoggerFactory.getLogger(DataPipelineController.class);

    DataPipelineStatusService dataPipelineStatusSvc;

    public DataPipelineController(DataPipelineStatusService dataPipelineStatusSvc) {
        this.dataPipelineStatusSvc = dataPipelineStatusSvc;
    }


    @GetMapping("/data-pipeline-status")
    @ResponseBody
    public ResponseEntity<String> getDataPipelineStatusHealth(){
       return this.dataPipelineStatusSvc.getHealthStatus();
    }

}
