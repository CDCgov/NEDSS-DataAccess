package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import lombok.Data;

@Data
public class InvestigationNotification {
    private Long investigationId;
    private Long notificationId;
}
