package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Data;

@Data
public class InvestigationObservation {
    private Long investigationId;
    private Long observationId;
}
