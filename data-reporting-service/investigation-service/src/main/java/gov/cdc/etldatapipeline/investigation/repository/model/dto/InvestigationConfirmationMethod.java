package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Data;

@Data
public class InvestigationConfirmationMethod {
    private Long investigationId;
    private String confirmationMethodCd;
    private String confirmationMethodDescTxt;
    private String confirmationMethodTime;
}
