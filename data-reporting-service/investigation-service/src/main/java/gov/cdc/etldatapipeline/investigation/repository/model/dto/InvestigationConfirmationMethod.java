package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class InvestigationConfirmationMethod {
    private Long investigationId;
    private String confirmationMethodCd;
    private String confirmationMethodDescTxt;
    private String confirmationMethodTime;
}
