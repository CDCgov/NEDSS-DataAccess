package gov.cdc.etldatapipeline.person.repository;

import gov.cdc.etldatapipeline.person.model.dto.patient.Patient;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PatientRepository extends JpaRepository<Patient, String> {
    @Query(nativeQuery = true, value = "execute sp_Patient_Event :person_uids")
    List<Patient> computePatients(@Param("person_uids") String person_uids);

}
