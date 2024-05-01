package gov.cdc.etldatapipeline.postprocessingservice.repository.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class OrganizationStoredProc {
    @Id
    private Long id;
}
