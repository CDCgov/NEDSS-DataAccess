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
public class OrganizationReporting implements DataRequiredFields {
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

    public static OrganizationReporting build(OrganizationSp orgSp) {
        return orgSp.processNestedJsonData(
                OrganizationReporting.builder()
                        .organizationUid(orgSp.getOrganizationUid())
                        .localId(orgSp.getLocalId())
                        .recordStatus(orgSp.getRecordStatusCd())
                        .generalComments(orgSp.getDescription())
                        .entryMethod(orgSp.getElectronicInd())
                        .standIndClass(orgSp.getStandIndClass())
                        .organizationName(orgSp.getOrganizationName())
                        .addTime(orgSp.getAddTime())
                        .addUserId(orgSp.getAddUserId())
                        .lastChgUserId(orgSp.getLastChgUserId())
                        .lastChgTime(orgSp.getLastChgTime())
                        .addUserName(orgSp.getAddUserName())
                        .lastChgUserName(orgSp.getLastChgUserName())
                        .build());
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("organizationUid");
    }
}
