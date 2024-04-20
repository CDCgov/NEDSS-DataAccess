package gov.cdc.etldatapipeline.postprocessingservice.repository;

import gov.cdc.etldatapipeline.postprocessingservice.repository.model.TestModel;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TestRepository extends JpaRepository<TestModel, String> {
    @Query(nativeQuery = true, value = "execute sp_nrt_patient_postprocessing :testIds")
//    @Procedure("sp_get_rdb_key")
    List<TestModel> computeIds(@Param("testIds") String testIds);
}
