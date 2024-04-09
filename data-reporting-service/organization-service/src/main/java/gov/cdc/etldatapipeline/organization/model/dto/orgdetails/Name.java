package gov.cdc.etldatapipeline.organization.model.dto.orgdetails;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgElasticSearch;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgReporting;
import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Name implements OrgExtension {
    private Long onOrgUid;
    private String organizationName;

    public <T> T updateOrg(T org) {
        if (org.getClass() == OrgReporting.class) {
            OrgReporting orgReporting = (OrgReporting) org;
            orgReporting.setOrganizationName(organizationName);
        } else if (org.getClass() == OrgElasticSearch.class) {
            OrgElasticSearch orgElastic = (OrgElasticSearch) org;
            orgElastic.setOrganizationName(organizationName);
            orgElastic.setOnOrgUid(onOrgUid);
        }
        return org;
    }
}
