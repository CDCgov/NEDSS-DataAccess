package gov.cdc.etldatapipeline.observation.repository;

import gov.cdc.etldatapipeline.observation.repository.model.Observation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IObservationRepository extends JpaRepository<Observation, String> {
    @Query(nativeQuery = true, value = "execute sp_Observation_Event :observation_uids")
    Optional<Observation> computePatients(@Param("observation_uids") String observation_uids);
}