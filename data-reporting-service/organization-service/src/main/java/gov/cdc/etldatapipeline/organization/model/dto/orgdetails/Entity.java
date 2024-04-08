package gov.cdc.etldatapipeline.organization.model.dto.orgdetails;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgElastic;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgReporting;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Entity implements OrgExtension {
    private Long entityUid;
    private String typeCd;
    private String recordStatusCd;
    private String rootExtensionTxt;
    private String entityIdSeq;
    private String assigningAuthorityCd;

    public <T> T updateOrg(T org) {
        if (org.getClass() == OrgReporting.class) {
            OrgReporting orgReporting = (OrgReporting) org;
            orgReporting.setQuickCode(rootExtensionTxt);
            //ToDo: Revisit logic
            orgReporting.setFacilityId(rootExtensionTxt);
            //ToDo: Revisit logic
            orgReporting.setFacilityIdAuth(rootExtensionTxt);
        } else if (org.getClass() == OrgElastic.class) {
            OrgElastic orgElastic = (OrgElastic) org;
            orgElastic.setTypeCd(typeCd);
            orgElastic.setEntityRecordStatusCd(recordStatusCd);
            orgElastic.setEntityUid(entityUid);
            orgElastic.setEntityIdSeq(entityIdSeq);
            orgElastic.setAssigningAuthorityCd(assigningAuthorityCd);
        }
        return org;
    }
}
