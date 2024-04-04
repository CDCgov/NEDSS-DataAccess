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
public class Fax implements OrgExtension {
    private String faxTlUid;
    private String faxElpCd;
    private String faxElpUseCd;
    private String orgFax;

    public <T> T updateOrg(T org) {
        if (org.getClass() == OrgReporting.class) {
            OrgReporting orgReporting = (OrgReporting) org;
            orgReporting.setFax(orgFax);
        } else if (org.getClass() == OrgElastic.class) {
            OrgElastic orgElastic = (OrgElastic) org;
            orgElastic.setFaxTlUid(faxTlUid);
            orgElastic.setFaxElpCd(faxElpCd);
            orgElastic.setFaxElpUseCd(faxElpUseCd);
            orgElastic.setFax(orgFax);
        }
        return org;
    }
}
