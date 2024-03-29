package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class InvestigationNotification {
    private Long investigationId;
    private Long notificationId;
}
