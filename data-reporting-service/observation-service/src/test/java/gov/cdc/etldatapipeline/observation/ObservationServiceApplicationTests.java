package gov.cdc.etldatapipeline.observation;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

@SpringBootTest
class ObservationServiceApplicationTests {

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoads() {
        assertNotNull(context, "The application context should not be null");
    }

    @Configuration
    static class TestConfiguration {

        @Bean
        @Primary
        public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
            return mock(LocalContainerEntityManagerFactoryBean.class);
        }
    }

}


//--DA_LOG_PATH="logs" --DB_ODSE="NBS_ODSE" --DB_PASSWORD="ods" --DB_URL="cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com" --DB_USERNAME="nbs_ods" --KAFKA_BOOTSTRAP_SERVER="localhost:9092"