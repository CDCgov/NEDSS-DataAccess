package gov.cdc.etldatapipeline.postprocessingservice.repository.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class ProviderStoredProc {
    @Id
    private Long id;
}
