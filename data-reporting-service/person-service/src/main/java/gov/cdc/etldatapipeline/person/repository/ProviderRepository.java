package gov.cdc.etldatapipeline.person.repository;

import gov.cdc.etldatapipeline.person.model.dto.Provider;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProviderRepository extends JpaRepository<Provider, String>  {
    @Query(nativeQuery = true, value = "execute sp_provider_event :person_uids")
    List<Provider> computeProviders(@Param("person_uids") String person_uids);

}
