package gov.cdc.etldatapipeline.changedata.model.dto;

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
public class ProviderFull extends Provider implements PersonExtendedProps{
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

    public void setRaceCd(String raceCd){}
    public void setRaceCategory(String raceCategoryCd){}
    public void setRaceDesc(String raceDescTxt){}

    /***
     * Transform the Name, Address,  Telephone, Email, EntityData(SSN), AddAuthUser, ChangeAuthUser
     * @return Fully Transformed Provider Object
     */
    public ProviderFull constructProviderFull(Provider p) {
        setPersonParentUid(p.getPersonUid());
        setPersonParentUid(p.getPersonParentUid());
        setDescription(p.getDescription());
        setAddTime(p.getAddTime());
        setFirstNm(p.getFirstNm());
        setMiddleNm(p.getMiddleNm());
        setLastNm(p.getLastNm());
        setNmSuffix(p.getNmSuffix());
        setCd(p.getCd());
        setElectronicInd(p.getElectronicInd());
        setLastChgTime(p.getLastChgTime());
        setRecordStatusCd(p.getRecordStatusCd());
        setRecordStatusTime(p.getRecordStatusTime());
        setStatusCd(p.getStatusCd());
        setStatusTime(p.getStatusTime());
        setLocalId(p.getLocalId());
        setVersionCtrlNbr(p.getVersionCtrlNbr());
        setEdxInd(p.getEdxInd());
        setDedupMatchInd(p.getDedupMatchInd());
        setAddUserId(p.getAddUserId());
        setLastChgUserId(p.getLastChgUserId());
        return this;
    }
}
