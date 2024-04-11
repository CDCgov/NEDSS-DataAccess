package gov.cdc.etldatapipeline.person.model.dto;


public interface PersonExtendedProps {
    default void setPnPersonUid(Long personUid) {
    }

    default void setPersonNmSeq(String personNameSeq) {
    }

    default void setNmUseCd(String personNameSeq) {
    }

    default void setNmDegree(String nmDegree) {
    }

    void setFirstNm(String firstNm);

    void setMiddleNm(String middleNm);

    void setLastNm(String lastNm);

    void setNmSuffix(String nmSuffix);

    default void setNmPrefix(String nmPrefix) {
    }

    void setStreetAddress1(String streetAddress1);

    void setStreetAddress2(String streetAddress2);

    void setCity(String city);

    default void setWithinCityLimits(String withInCityLimits) {
    }

    void setState(String state);

    void setStateCode(String stateCode);

    void setZip(String zip);

    default void setCounty(String county) {
    }

    void setCountyCode(String county_code);

    default void setCountryCode(String countryCode) {
    }

    default void setCountry(String homeCountry) {
    }

    default void setHomeCountry(String homeCountry) {
    }

    default void setBirthCountry(String birthCountry) {
    }

    default void setAddressComments(String birthCountry) {
    }

    default void setAddrElpCd(String elpCd) {
    }

    default void setAddrElpUseCd(String elpUseCd) {
    }

    default void setAddrPlUid(Long postalLocatorUid) {
    }

    default void setPhoneWork(String phoneWork) {
    }

    default void setPhoneExtWork(String phoneExtWork) {
    }

    default void setPhoneHome(String phoneHome) {
    }

    default void setPhoneExtHome(String phoneExtHome) {
    }

    default void setPhoneCell(String phoneCell) {
    }

    default void setPhElpCd(String phElpCd) {
    }

    default void setPhElpUseCd(String phElpUseCd) {
    }

    default void setPhTlUid(Long phTlUid) {
    }

    void setEmail(String email);

    default void setEmailElpCd(String phElpCd) {
    }

    default void setEmailElpUseCd(String phElpUseCd) {
    }

    default void setEmailTlUid(Long phTlUid) {
    }

    default void setSsn(String ssn) {
    }

    default void setRaceCd(String raceCd) {
    }

    default void setRaceCategory(String raceCategory) {
    }

    default void setRaceDesc(String raceDesc) {
    }

    default void setPrPersonUid(Long prPersonUid) {
    }

    default void setPatientNumber(String patientNumber) {
    }

    default void setPatientNumberAuth(String patientNumberAuth) {
    }

    default void setProviderQuickCode(String providerQuickCode) {
    }

    default void setProviderRegistrationNum(String providerRegistrationNum) {
    }

    default void setProviderRegistrationNumAuth(String providerRegistrationNumAuth) {
    }

    default void setEntityUid(Long entityUid) {
    }

    default void setTypeCd(String typeCd) {
    }

    default void setEntityRecordStatusCd(String recordStatusCd) {
    }

    default void setEntityIdSeq(Integer entityIdSeq) {
    }

    default void setAssigningAuthorityCd(String assigningAuthorityCd) {
    }

}

