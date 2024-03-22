package gov.cdc.etldatapipeline.observation.repository.model;

import lombok.*;

@NoArgsConstructor
@Getter
@Setter
@ToString
public class ObservationTransformed {
    private String orderingPersonId;
    private String patientId;
    private String performingOrganizationId;
    private String authorOrganizationId;
    private String orderingOrganizationId;
    private String materialId;
    private String resultObservationUid;
}
