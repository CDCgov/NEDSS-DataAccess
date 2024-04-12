package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Data;

@Data
public class InvestigationTransformed {
    private Long investigatorId;
    private Long physicianId;
    private Long patientId;
    private Long organizationId;
    private Long invStateCaseId;
    private Long cityCountyCaseNbr;
    private Long legacyCaseId;
    private Long phcInvFormId;
}
