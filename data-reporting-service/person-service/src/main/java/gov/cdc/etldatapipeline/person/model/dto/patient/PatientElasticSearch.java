package gov.cdc.etldatapipeline.person.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.person.model.dto.DataRequiredFields;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class PatientElasticSearch implements PersonExtendedProps, DataRequiredFields {
    private Long patientUid;
    private String additionalGenderCd;
    private String adultsInHouseNbr;
    private String asOfDateAdmin;
    private String asOfDateEthnicity;
    private String asOfDateGeneral;
    private String asOfDateMorbidity;
    private String asOfDateSex;
    private String ageReported;
    private String ageReportedUnitCd;
    private String addTime;
    private Long addUserId;
    private String birthOrderNbr;
    private String birthSex;
    private String birthTime;
    private String cd;
    private String childrenInHouseNbr;
    private String currSexCd;
    private String deceasedIndCd;
    private String deceasedTime;
    private String dedupMatchInd;
    private String description;
    private String educationLevelCd;
    private String edxInd;
    private String electronicInd;
    private String ethnicGroupInd;
    private String ethnicUnkReasonCd;
    private Long lastChgUserId;
    private String lastChgTime;
    private String localId;
    private String maritalStatusCd;
    private String multipleBirthInd;
    private String occupationCd;
    private String personFirstNm;
    private String personLastNm;
    private String personMiddleNm;
    private String personNmSuffix;
    private Long personParentUid;
    private String preferredGenderCd;
    private String primLangCd;
    private String recordStatusCd;
    private String recordStatusTime;
    private String sexUnkReasonCd;
    private String speaksEnglishCd;
    private String statusCd;
    private String statusTime;
    private String versionCtrlNbr;


    //Name from Person_Name ODSE Table
    @JsonProperty("firstnm")
    private String firstnm;
    @JsonProperty("middleNm")
    private String middleNm;
    @JsonProperty("lastNm")
    private String lastNm;
    @JsonProperty("nmSuffix")
    private String nmSuffix;
    @JsonProperty("nmdegree")
    private String nmdegree;
    private String personNmSeq;
    private String nmUseCd;

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
    private String elpCd;
    private String elpUseCd;
    private String postalLocatorUid;


    //Phone
    private String telephoneNbr;
    private String extensionTxt;
    private String phElpCd;
    private String phElpUseCd;
    private String phTlUid;

    //Email
    private String email;
    private String emailElpCd;
    private String emailElpUseCd;
    private String emailTlUid;


    //Race
    private String raceCd;
    private String raceCategory;
    private String raceDesc;
    private String raceCodeDescTxt;
    private String raceCodeParentIsCd;
    private String prPersonUid;

    //EntityId
    private String typeCd;
    private String personEntityRecordStatusCd;
    private String entityUid;
    private String entityIdSeq;
    private String assigningAuthorityCd;

    public PatientElasticSearch constructObject(Patient p) {
        setPatientUid(p.getPatientUid());
        setAdditionalGenderCd(p.getAdditionalGenderCd());
        setAddUserId(p.getAddUserId());
        setAgeReported(p.getAgeReported());
        setAgeReportedUnitCd(p.getAgeReportedUnitCd());
        setAddTime(p.getAddTime());
        setBirthSex(p.getBirthGenderCd());
        setBirthTime(p.getBirthTime());
        setCurrSexCd(p.getCurrSexCd());
        setDeceasedTime(p.getDeceasedTime());
        setDescription(p.getDescription());
        setElectronicInd(p.getElectronicInd());
        setEthnicGroupInd(p.getEthnicGroupInd());
        setEthnicUnkReasonCd(p.getEthnicUnkReasonCd());
        setLastChgUserId(p.getLastChgUserId());
        setLastChgTime(p.getLastChgTime());
        setLocalId(p.getLocalId());
        setMaritalStatusCd(p.getMaritalStatusCd());
        setOccupationCd(p.getOccupationCd());
        setPersonParentUid(p.getPersonParentUid());
        setPreferredGenderCd(p.getPreferredGenderCd());
        setPrimLangCd(p.getPrimLangCd());
        setRecordStatusCd(p.getRecordStatusCd());
        setSexUnkReasonCd(p.getSexUnkReasonCd());
        setSpeaksEnglishCd(p.getSpeaksEnglishCd());
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
        setFirstnm(firstNm);
    }

    @Override
    public void setPersonNameMiddleNm(String middleNm) {
        setMiddleNm(middleNm);
    }

    @Override
    public void setPersonNameLastNm(String lastNm) {
        setLastNm(lastNm);
    }

    @Override
    public void setPersonNameNmSuffix(String nmSuffix) {
        setNmSuffix(nmSuffix);
    }
}
