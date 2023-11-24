package gov.cdc.etldatapipeline;

import gov.cdc.etldatapipeline.changedata.controller.DataPipelineController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class EtlDataPipelineApplicationTests {

    @Autowired
    private DataPipelineController controller;

    @Test
    void contextLoads() {
        assertThat(controller).isNotNull();
    }
}
