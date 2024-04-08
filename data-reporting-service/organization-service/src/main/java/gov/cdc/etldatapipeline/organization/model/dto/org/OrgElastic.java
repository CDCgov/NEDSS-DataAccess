package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
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
    @JsonProperty("record_status_cd")
    private String orgRecordStatusCd;
    private String description;
    private String electronicInd;
    private String standIndClass;
    private Long onOrgUid;
    private String organizationName;
    private String typeCd;
    @JsonProperty("recordStatusCd")
    private String entityRecordStatusCd;
    private Long entityUid;
    private String entityIdSeq;
    private String assigningAuthorityCd;
    private String addrElpCd;
    private String addrElpUseCd;
    private Long addrPlUid;
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
    private Long phTlUid;
    @JsonProperty("telephoneNbr")
    private String telephoneNbr;
    @JsonProperty("extensionTxt")
    private String extensionTxt;
    @JsonProperty("emailAddress")
    private String emailAddress;
    private String phoneComments;
    private String faxElpCd;
    private String faxElpUseCd;
    private Long faxTlUid;
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
        setOrgRecordStatusCd(orgSp.getRecordStatusCd());
        setDescription(orgSp.getDescription());
        setElectronicInd(orgSp.getElectronicInd());
        setStandIndClass(orgSp.getStandIndClass());
        setAddUserId(orgSp.getAddUserId());
        setAddTime(orgSp.getAddTime());
        setLastChgUserId(orgSp.getLastChgUserId());
        setLastChgTime(orgSp.getLastChgTime());
        orgSp.processNestedJsonData(this);
        return this;
    }

    public static OrgElastic build(OrgSp p) {
        return OrgElastic.builder().build().constructObject(p);
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("organizationUid");
    }
}
