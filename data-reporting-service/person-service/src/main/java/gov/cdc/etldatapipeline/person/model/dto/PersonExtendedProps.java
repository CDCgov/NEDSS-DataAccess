package gov.cdc.etldatapipeline.person.model.dto;


public interface PersonExtendedProps {
    void setPersonNameFirstNm(String firstNm);

    void setPersonNameMiddleNm(String middleNm);

    void setPersonNameLastNm(String lastNm);

    void setPersonNameNmSuffix(String nmSuffix);

    void setStreetAddress1(String streetAddress1);

    void setStreetAddress2(String streetAddress2);

    void setCity(String city);

    void setState(String state);

    void setStateCode(String stateCode);

    void setZip(String zip);

    void setCounty(String county);

    void setCountyCode(String county_code);

    default void setCountryCode(String countryCode) {
    }

    default void setHomeCountry(String homeCountry) {
    }

    default void setBirthCountry(String birthCountry) {
    }

    default void setAddressComments(String birthCountry) {
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

    void setEmail(String email);

    default void setSsn(String ssn) {
    }

    default void setRaceCd(String raceCd) {
    }

    default void setRaceCategory(String raceCategory) {
    }

    default void setRaceDesc(String raceDesc) {
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
}

