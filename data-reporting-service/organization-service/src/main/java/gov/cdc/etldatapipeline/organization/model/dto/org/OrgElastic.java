package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import gov.cdc.etldatapipeline.organization.utils.DataPostProcessor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Set;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrgElastic implements DataRequiredFields {
    private long organizationUid;
    private String cd;
    private String statusCd;
    private String statusTime;
    private String versionCtrlNbr;
    private String edxInd;
    private String recordStatusTime;
    private String localId;
    private String recordStatusCd;
    private String description;
    private String electronicInd;
    private String standIndClass;
    private String onOrgUid;
    private String organizationName;
    private String typeCd;
    @JsonProperty("recordStatusCd")
    private String entityRecordStatusCd;
    private String entityUid;
    private String entityIdSeq;
    private String assigningAuthorityCd;
    private String addrElpCd;
    private String addrElpUseCd;
    private String addrPlUid;
    @JsonProperty("streetAddr1")
    private String streetAddr1;
    @JsonProperty("streetAddr2")
    private String streetAddr2;
    private String city;
    private String stateDesc;
    private String state;
    private String zip;
    @JsonProperty("cntyCd")
    private String cntyCd;
    @JsonProperty("cntryCd")
    private String cntryCd;
    private String addressComments;
    private String phElpCd;
    private String phElpUseCd;
    private String phTlUid;
    @JsonProperty("telephoneNbr")
    private String telephoneNbr;
    @JsonProperty("extensionTxt")
    private String extensionTxt;
    @JsonProperty("emailAddress")
    private String emailAddress;
    private String phoneComments;
    private String faxElpCd;
    private String faxElpUseCd;
    private String faxTlUid;
    @JsonProperty("org_fax")
    private String fax;
    private Long addUserId;
    private String addTime;
    private Long lastChgUserId;
    private String lastChgTime;

    public OrgElastic constructObject(OrgSp orgSp) {
        setOrganizationUid(orgSp.getOrganizationUid());
        setCd(orgSp.getCd());
        setStatusCd(orgSp.getStatusCd());
        setStatusTime(orgSp.getStatusTime());
        setVersionCtrlNbr(orgSp.getVersionCtrlNbr());
        setEdxInd(orgSp.getEdxInd());
        setRecordStatusTime(orgSp.getRecordStatusTime());
        setLocalId(orgSp.getLocalId());
        setRecordStatusCd(orgSp.getRecordStatusCd());
        setDescription(orgSp.getDescription());
        setElectronicInd(orgSp.getElectronicInd());
        setStandIndClass(orgSp.getStandIndClass());
        setAddUserId(orgSp.getAddUserId());
        setAddTime(orgSp.getAddTime());
        setLastChgUserId(orgSp.getLastChgUserId());
        setLastChgTime(orgSp.getLastChgTime());

        new DataPostProcessor().processAllProps(
                orgSp.getOrganizationFax(),
                orgSp.getOrganizationAddress(),
                orgSp.getOrganizationTelephone(),
                orgSp.getOrganizationEntityId(),
                orgSp.getOrganizationName(),
                this);
        return this;
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("organizationUid");
    }
}
