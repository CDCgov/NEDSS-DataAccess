package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Set;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrgReporting implements DataRequiredFields {
    private Long organizationUid;
    private String localId;
    private String recordStatus;
    private String generalComments;
    private String entryMethod;
    private String standIndClass;
    private String organizationName;
    private String quickCode;
    private String facilityId;
    private String facilityIdAuth;
    @JsonProperty("street_address_1")
    private String streetAddress1;
    @JsonProperty("street_address_2")
    private String streetAddress2;
    private String city;
    private String state;
    private String stateCode;
    private String zip;
    private String county;
    private String countyCode;
    private String country;
    private String addressComments;
    private String phoneWork;
    private String phoneExtWork;
    private String email;
    private String phoneComments;
    private String fax;
    private Long addUserId;
    private String addUserName;
    private String addTime;
    private Long lastChgUserId;
    private String lastChgUserName;
    private String lastChgTime;
    private String refreshDatetime;

    public OrgReporting constructObject(OrgSp orgSp) {
        setOrganizationUid(orgSp.getOrganizationUid());
        setLocalId(orgSp.getLocalId());
        setRecordStatus(orgSp.getRecordStatusCd());
        setGeneralComments(orgSp.getDescription());
        setEntryMethod(orgSp.getElectronicInd());
        setStandIndClass(orgSp.getStandIndClass());
        setOrganizationName(orgSp.getOrganizationName());
        setAddTime(orgSp.getAddTime());
        setAddUserId(orgSp.getAddUserId());
        setLastChgUserId(orgSp.getLastChgUserId());
        setLastChgTime(orgSp.getLastChgTime());
        setAddUserName(orgSp.getAddUserName());
        setLastChgUserName(orgSp.getLastChgUserName());
        orgSp.processNestedJsonData(this);
        return this;
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("organizationUid");
    }
}
