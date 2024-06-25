package gov.cdc.etldatapipeline.postprocessingservice.repository;


import gov.cdc.etldatapipeline.postprocessingservice.repository.model.InvestigationStoredProc;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface InvestigationRepository extends JpaRepository<InvestigationStoredProc, Long> {
    @Procedure("sp_nrt_investigation_postprocessing")
    void executeStoredProcForPublicHealthCaseIds(@Param("publicHealthCaseUids") String publicHealthCaseUids);

    @Procedure("sp_f_page_case_postprocessing")
    void executeStoredProcForFPageCase(@Param("publicHealthCaseUids") String publicHealthCaseUids);
}
