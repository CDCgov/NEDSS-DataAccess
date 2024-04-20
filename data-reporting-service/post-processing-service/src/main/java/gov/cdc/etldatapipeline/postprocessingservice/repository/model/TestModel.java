package gov.cdc.etldatapipeline.postprocessingservice.repository.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class TestModel {
    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "pat_key")
    private String patKey;
}
