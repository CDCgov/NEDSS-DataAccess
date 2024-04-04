package gov.cdc.etldatapipeline.organization.model.dto.orgdetails;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgElastic;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgReporting;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Name implements OrgExtension {
    private String onOrgUid;
    private String organizationName;

    public <T> T updateOrg(T org) {
        if (org.getClass() == OrgReporting.class) {
            OrgReporting orgReporting = (OrgReporting) org;
            orgReporting.setOrganizationName(organizationName);
        } else if (org.getClass() == OrgElastic.class) {
            OrgElastic orgElastic = (OrgElastic) org;
            orgElastic.setOrganizationName(organizationName);
            orgElastic.setOnOrgUid(onOrgUid);
        }
        return org;
    }
}
