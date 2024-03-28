package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
public class InvestigationObservation {
    private Long investigationId;
    private List<Long> observationId;
}
