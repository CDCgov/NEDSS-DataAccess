package gov.cdc.etldatapipeline.organization.repository;

import gov.cdc.etldatapipeline.organization.model.dto.org.OrgSp;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public interface OrgRepository extends JpaRepository<OrgSp, String> {

    @Query(nativeQuery = true, value = "execute sp_organization_event :org_uids")
    Set<OrgSp> computeAllOrganizations(@Param("org_uids") String org_ids);

}
