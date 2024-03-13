package gov.cdc.etldatapipeline.changedata.repository;

import gov.cdc.etldatapipeline.changedata.model.dto.Patient;
import gov.cdc.etldatapipeline.changedata.model.dto.ProviderFull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProviderRepository extends JpaRepository<Patient, String>  {
    @Query(nativeQuery = true, value = "execute sp_PROVIDER_EVENT :person_uids")
    List<ProviderFull> computeProviders(@Param("person_uids") String person_uids);

}
