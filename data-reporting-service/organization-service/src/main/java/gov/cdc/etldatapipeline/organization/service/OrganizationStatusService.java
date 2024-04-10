package gov.cdc.etldatapipeline.organization.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class OrganizationStatusService {
    private static final Logger LOG = LoggerFactory.getLogger(OrganizationStatusService.class);

    public OrganizationStatusService() {
    }

    public ResponseEntity<String> getHealthStatus() {
        LOG.info("Organization Service Status OK");
        return ResponseEntity.status(HttpStatus.OK).body("Organization Service Status OK");
    }
}
