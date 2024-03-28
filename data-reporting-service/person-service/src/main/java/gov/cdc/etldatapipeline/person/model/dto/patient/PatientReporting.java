package gov.cdc.etldatapipeline.person.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.person.model.dto.DataRequiredFields;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class PatientReporting implements PersonExtendedProps, DataRequiredFields {
    private Long patientUid;
    private String addlGenderInfo;
    private String ageReported;
    private String ageReportedUnit;
    private String addTime;
    private Long addUserId;
    private String birth_sex;
    private String dob;
    private String currentSex;
    private String deceasedIndicator;
    private String patientDeceasedDate;
    private String generalComments;
    private String electronicInd;
    private String ethnicity;
    private String unkEthnicRsn;
    private Long lastChgUserId;
    private String lastChgTime;
    private String localId;
    private String maritalStatus;
    private String primaryOccupation;
    private Long patientMprUid;
    private String preferredGender;
    private String primaryLanguage;
    private String recordStatus;
    private String currSexUnkRsn;
    private String speaksEnglish;

    // From Function AUTH_USR
    private String addedBy;
    private String lastUpdatedBy;

    //Name from Person_Name ODSE Table
    private String firstName;
    private String middleName;
    private String lastName;
    private String nameSuffix;
    private String aliasNickname;
    private String personNmSeq;

    //Address
    private String streetAddress1;
    private String streetAddress2;
    private String city;
    private String state;
    private String stateCode;
    private String zip;
    private String county;
    private String countyCode;
    private String countryCode;
    private String withinCityLimits;
    /* @JsonProperty("home_country") // ToDo: Do you want to have home_country since birth_country is provided.
     private String homeCountry;*/
    private String birthCountry;
    private String censusTract;


    //Phone
    private String phoneWork;
    private String phoneExtWork;
    private String phoneHome;
    private String PhoneExtHome;
    private String PhoneCell;

    //Email
    private String email;

    //Entity
    private String ssn;
    private String patientNumber;
    private String patientNumberAuth;

    //Race
    private String raceCalculated;
    private String raceCalcDetails;
    private String raceAll;
    private String raceAmerInd1;
    private String raceAmerInd2;
    private String raceAmerInd3;
    private String raceAmerIndGt3Ind;
    private String raceAmerIndAll;
    private String raceAsian1;
    private String raceAsian2;
    private String raceAsian3;
    private String raceAsianGt3Ind;
    private String raceAsianAll;
    private String raceBlack1;
    private String raceBlack2;
    private String raceBlack3;
    private String raceBlackGt3Ind;
    private String raceBlackAll;
    private String raceNatHi1;
    private String raceNatHi2;
    private String raceNatHi3;
    private String raceNatHiGt3Ind;
    private String raceNatHiAll;
    private String raceWhite1;
    private String raceWhite2;
    private String raceWhite3;
    private String raceWhiteGt3Ind;
    private String raceWhiteAll;

    public PatientReporting constructPatientReporting(Patient p) {
        setPatientUid(p.getPatientUid());
        setAddlGenderInfo(p.getAdditionalGenderCd());
        setAddUserId(p.getAddUserId());
        setAgeReported(p.getAgeReported());
        setAgeReportedUnit(p.getAgeReportedUnitCd());
        setAddTime(p.getAddTime());
        setBirth_sex(p.getBirthGenderCd());
        setDob(p.getBirthTime());
        setCurrentSex(p.getCurrSexCd());
        setDeceasedIndicator(p.getDeceasedIndCd());
        setPatientDeceasedDate(p.getDeceasedTime());
        setGeneralComments(p.getDescription());
        setElectronicInd(p.getElectronicInd());
        setEthnicity(p.getEthnicGroupInd());
        setUnkEthnicRsn(p.getEthnicUnkReasonCd());
        setLastChgUserId(p.getLastChgUserId());
        setLastChgTime(p.getLastChgTime());
        setLocalId(p.getLocalId());
        setMaritalStatus(p.getMaritalStatusCd());
        setPrimaryOccupation(p.getOccupationCd());
        setPatientMprUid(p.getPersonParentUid());
        setPreferredGender(p.getPreferredGenderCd());
        setPrimaryLanguage(p.getPrimLangCd());
        setRecordStatus(p.getRecordStatusCd());
        setCurrSexUnkRsn(p.getSexUnkReasonCd());
        setSpeaksEnglish(p.getSpeaksEnglishCd());

        // Fn() - Auth_User
        setAddedBy(p.getAddUserName());
        setLastUpdatedBy(p.getLastChgUserName());
        return this;
    }

    /**
     * List of Required Fields
     *
     * @return Required Fields
     */
    public Set<String> getRequiredFields() {
        return Set.of("patientUid");
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
}
