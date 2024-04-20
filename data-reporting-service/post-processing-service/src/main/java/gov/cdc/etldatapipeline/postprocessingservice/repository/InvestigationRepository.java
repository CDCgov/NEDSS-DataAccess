package gov.cdc.etldatapipeline.postprocessingservice.repository;

import gov.cdc.etldatapipeline.postprocessingservice.repository.model.InvestigationStoredProc;
import gov.cdc.etldatapipeline.postprocessingservice.repository.model.PatientStoredProc;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface InvestigationRepository extends JpaRepository<InvestigationStoredProc, Long> {
    @Query(nativeQuery = true, value = "select * from d_patient")
    List<PatientStoredProc> executeStoredProcForInvestigationIds(@Param("investigation_uids") List<Long> publicHealthCaseUids);
}

