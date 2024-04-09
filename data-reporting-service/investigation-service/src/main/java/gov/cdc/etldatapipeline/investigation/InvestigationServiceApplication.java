package gov.cdc.etldatapipeline.investigation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"gov.cdc.etldatapipeline.commonutil"})
public class InvestigationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InvestigationServiceApplication.class, args);
    }

}
