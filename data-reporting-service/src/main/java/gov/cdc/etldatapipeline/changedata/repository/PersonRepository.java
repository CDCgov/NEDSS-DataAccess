package gov.cdc.etldatapipeline.changedata.repository;

import gov.cdc.etldatapipeline.changedata.model.dto.PersonOp;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PersonRepository extends JpaRepository<PersonOp, String>  {
    @Query(nativeQuery = true, value = "execute sp_Person_Event :person_uids")
    List<PersonOp> computeAllPatients(@Param("person_uids") String person_uids);

}
