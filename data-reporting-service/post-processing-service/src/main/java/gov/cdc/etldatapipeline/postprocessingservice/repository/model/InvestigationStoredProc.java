package gov.cdc.etldatapipeline.postprocessingservice.repository.model;


import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Data
@Entity
public class InvestigationStoredProc {
    @Id
    private Long id;
}
