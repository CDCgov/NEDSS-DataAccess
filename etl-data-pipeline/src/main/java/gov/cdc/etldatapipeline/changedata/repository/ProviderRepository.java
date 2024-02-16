package gov.cdc.etldatapipeline.changedata.repository;

import gov.cdc.etldatapipeline.changedata.model.dto.Provider;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProviderRepository extends JpaRepository<Provider, String> {
    @Query(nativeQuery = true, value = "execute sp_D_PROVIDER_EVENT :person_uids")
    List<Provider> getAllProviders(@Param("person_uids") String provider_ids);
}
