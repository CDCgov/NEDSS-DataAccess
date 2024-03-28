package gov.cdc.etldatapipeline.investigation.repository.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class InvestigationTransformed {
    private Long investigatorId;
    private Long physicianId;
    private Long patientId;
    private Long organizationId;
    private String invStateCaseId;
    private String cityCountyCaseNbr;
    private String legacyCaseId;
    private Long phcInvFormId;
    private String investigationObservation;
    private String investigationNotification;
    private String investigationConfirmationMethod;
}
