package gov.cdc.etldatapipeline.changedata.repository;

import gov.cdc.etldatapipeline.changedata.model.rdb.InitPatient;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PatientRepository extends JpaRepository<InitPatient, String>  {
}
