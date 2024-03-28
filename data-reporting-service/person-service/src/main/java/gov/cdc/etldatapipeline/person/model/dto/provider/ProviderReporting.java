package gov.cdc.etldatapipeline.person.model.dto.provider;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.person.model.dto.DataRequiredFields;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Set;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ProviderReporting implements PersonExtendedProps, DataRequiredFields {
    private Long providerUid;
    private String localId;
    private String recordStatus;
    private String entryMethod;
    private String generalComments;
    private Long addUserId;
    private Long lastChgUserId;
    private String addUserName;
    private String addTime;
    private String lastChgUserName;
    private String lastChgTime;

    //Name from Person_Name ODSE Table
    private String firstName;
    private String middleName;
    private String lastName;
    private String nameSuffix;
    private String nameDegree;

    //Address
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


    //Phone
    private String phoneWork;
    private String phoneExtWork;
    private String phoneComments;
    private String phoneCell;

    //Email
    private String emailWork;

    //Entity
    private String providerQuickCode;
    private String providerRegistrationNum;
    private String providerRegistrationNumAuth;

    /***
     * Transform the Name, Address,  Telephone, Email, EntityData(SSN), AddAuthUser, ChangeAuthUser
     * @return Fully Transformed Provider Object
     */
    public ProviderReporting constructProviderFull(Provider p) {
        setProviderUid(p.getPersonUid());
        setLocalId(p.getLocalId());
        setRecordStatus(p.getRecordStatusCd());
        setEntryMethod(p.getElectronicInd());
        setGeneralComments(p.getDescription());
        setAddUserId(p.getAddUserId());
        setLastChgUserId(p.getLastChgUserId());
        setAddUserName(p.getAddUserName());
        setLastChgUserName(p.getLastChgUserName());
        setLastChgTime(p.getLastChgTime());
        setAddTime(p.getAddTime());
        return this;
    }

    /**
     * List of Required Fields
     *
     * @return Required Fields
     */
    @Override
    public Set<String> getRequiredFields() {
        return Set.of("providerUid");
    }

    @Override
    public void setPersonNameFirstNm(String firstNm) {
        setFirstName(firstNm);
    }

    @Override
    public void setPersonNameMiddleNm(String middleNm) {
        setMiddleName(middleNm);
    }

    @Override
    public void setPersonNameLastNm(String lastNm) {
        setLastName(lastNm);
    }

    @Override
    public void setPersonNameNmSuffix(String nmSuffix) {
        setNameSuffix(nmSuffix);
    }

    @Override
    public void setCountryCode(String countryCode) {
        setCountry(countryCode);
    }

    @Override
    public void setEmail(String email) {
        setEmailWork(email);
    }

    @Override
    public void setHomeCountry(String homeCountry) {
        setCountry(homeCountry);
    }
}
