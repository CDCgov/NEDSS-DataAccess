package gov.cdc.etldatapipeline.investigation.repository;

import gov.cdc.etldatapipeline.investigation.repository.model.Investigation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface InvestigationRepository extends JpaRepository<Investigation, String> {
}
