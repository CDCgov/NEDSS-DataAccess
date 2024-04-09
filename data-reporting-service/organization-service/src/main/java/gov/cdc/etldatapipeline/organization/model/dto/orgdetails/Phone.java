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
public class Phone implements OrgExtension {
    private Long phTlUid;
    private String phElpCd;
    private String phElpUseCd;
    private String telephoneNbr;
    private String extensionTxt;
    private String emailAddress;
    private String phone_comments;

    public <T> T updateOrg(T org) {
        if (org.getClass() == OrgReporting.class) {
            OrgReporting orgReporting = (OrgReporting) org;
            orgReporting.setPhoneWork(telephoneNbr);
            orgReporting.setPhoneExtWork(extensionTxt);
            orgReporting.setPhoneComments(phone_comments);
            orgReporting.setEmail(emailAddress);
        } else if (org.getClass() == OrgElasticSearch.class) {
            OrgElasticSearch orgElastic = (OrgElasticSearch) org;
            orgElastic.setPhElpCd(phElpCd);
            orgElastic.setPhElpUseCd(phElpUseCd);
            orgElastic.setPhTlUid(phTlUid);
            orgElastic.setTelephoneNbr(telephoneNbr);
            orgElastic.setExtensionTxt(extensionTxt);
            orgElastic.setPhoneComments(phone_comments);
            orgElastic.setEmailAddress(emailAddress);
        }
        return org;
    }
}
