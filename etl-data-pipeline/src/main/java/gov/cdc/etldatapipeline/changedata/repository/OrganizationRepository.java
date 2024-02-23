package gov.cdc.etldatapipeline.changedata.repository;

import gov.cdc.etldatapipeline.changedata.model.dto.OrganizationOP;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrganizationRepository extends JpaRepository<OrganizationOP, String> {

    @Query(nativeQuery = true, value = "execute sp_Organization :org_uids")
    List<OrganizationOP> computeAllOrganizations(@Param("org_uids") String org_ids);

}
