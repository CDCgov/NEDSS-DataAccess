package gov.cdc.datareportingservice.changedata.repository;

import gov.cdc.datareportingservice.changedata.model.dto.Provider;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProviderRepository extends JpaRepository<Provider, String> {

    @Query(nativeQuery = true, value = "execute sp_PROVIDER_EVENT :person_uids")
    List<Provider> computeAllProviders(@Param("person_uids") String provider_ids);

}
