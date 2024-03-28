package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
public class InvestigationConfirmation {
    private Long investigationId;
    private String confirmationMethodCd;
    private String confirmationMethodDescTxt;
//    private Map<String, String> confirmationMethodMap;
    private String confirmationMethodTime;
}
