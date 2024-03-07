package gov.cdc.etldatapipeline.changedata.repository;

import gov.cdc.etldatapipeline.changedata.model.dto.PatientOP;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PatientRepository extends JpaRepository<PatientOP, String>  {
    @Query(nativeQuery = true, value = "execute sp_Patient_Info :person_uids")
    List<PatientOP> computeAllPatients(@Param("person_uids") String person_uids);

}
