package gov.cdc.etldatapipeline.person.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(callSuper=true)
public class PatientFull extends Patient implements PersonExtendedProps {
    private String streetAddress1;
    private String streetAddress2;
    private String city;
    private String state;
    private String stateCode;
    private String zip;
    private String county;
    private String countyCode;
    private String country;
    private String countryCode;
    private String birthCountry;
    private String phoneWork;
    private String phoneExtWork;
    private String phoneHome;
    private String phoneExtHome;
    private String phoneCell;
    private String email;
    private String ssn;
    private Long addedBy;
    private Long lastChangedBy;
    private String raceCd;
    private String raceCategory;
    private String raceDesc;
    private String patientNumber;
    private String patientNumberAuth;
    private String providerQuickCode;
    private String providerRegistrationNum;
    private String providerRegistrationNumAuth;

    public PatientFull constructPersonFull(Patient p) {
        setPersonUid(p.getPersonUid());
        setPersonParentUid(p.getPersonParentUid());
        setDescription(p.getDescription());
        setAddTime(p.getAddTime());
        setAgeReported(p.getAgeReported());
        setAgeReportedUnitCd(p.getAgeReportedUnitCd());
        setFirstNm(p.getFirstNm());
        setMiddleNm(p.getMiddleNm());
        setLastNm(p.getLastNm());
        setNmSuffix(p.getNmSuffix());
        setAsOfDateMorbidity(p.getAsOfDateAdmin());
        setAsOfDateEthnicity(p.getAsOfDateEthnicity());
        setAsOfDateGeneral(p.getAsOfDateGeneral());
        setAsOfDateMorbidity(p.getAsOfDateMorbidity());
        setAsOfDateSex(p.getAsOfDateSex());
        setBirthTime(p.getBirthTime());
        setBirthTimeCalc(p.getBirthTimeCalc());
        setCd(p.getCd());
        setCurrSexCd(p.getCurrSexCd());
        setDeceasedIndCd(p.getDeceasedIndCd());
        setElectronicInd(p.getElectronicInd());
        setEthnicGroupInd(p.getEthnicGroupInd());
        setLastChgTime(p.getLastChgTime());
        setMaritalStatusCd(p.getMaritalStatusCd());
        setRecordStatusCd(p.getRecordStatusCd());
        setRecordStatusTime(p.getRecordStatusTime());
        setStatusCd(p.getStatusCd());
        setStatusTime(p.getStatusTime());
        setLocalId(p.getLocalId());
        setVersionCtrlNbr(p.getVersionCtrlNbr());
        setEdxInd(p.getEdxInd());
        setDedupMatchInd(p.getDedupMatchInd());
        setSpeaksEnglishCd(p.getSpeaksEnglishCd());
        setEthnicUnkReasonCd(p.getEthnicUnkReasonCd());
        setSexUnkReasonCd(p.getSexUnkReasonCd());
        setPreferredGenderCd(p.getPreferredGenderCd());
        setAdditionalGenderCd(p.getAdditionalGenderCd());
        setOccupationCd(p.getOccupationCd());
        setPrimLangCd(p.getPrimLangCd());
        setAddUserId(p.getAddUserId());
        setLastChgUserId(p.getLastChgUserId());
        setMultipleBirthInd(p.getMultipleBirthInd());
        setAdultsInHouseNbr(p.getAdultsInHouseNbr());
        setBirthOrderNbr(p.getBirthOrderNbr());
        setChildrenInHouseNbr(p.getChildrenInHouseNbr());
        setEducationLevelCd(p.getEducationLevelCd());
        return this;
    }
}
