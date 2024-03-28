package gov.cdc.etldatapipeline.person.model.dto;


public interface PersonExtendedProps {
    void setPersonNameFirstNm(String firstNm);

    void setPersonNameMiddleNm(String middleNm);

    void setPersonNameLastNm(String lastNm);

    void setPersonNameNmSuffix(String nmSuffix);

    void setStreetAddress1(String street_address_1);

    void setStreetAddress2(String street_address_2);

    void setCity(String city);

    void setState(String state);

    void setStateCode(String stateCode);

    void setZip(String zip);

    void setCounty(String county);

    void setCountyCode(String county_code);

    void setCountryCode(String countryCode);

    default void setBirthCountry(String birth_country) {
    }

    default void setPhoneWork(String phone_work) {
    }

    default void setPhoneExtWork(String phone_ext_work) {
    }

    default void setPhoneHome(String phone_home) {
    }

    default void setPhoneExtHome(String phone_ext_home) {
    }

    default void setPhoneCell(String phone_cell) {
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

    default void setPatientNumber(String patient_number) {
    }

    default void setPatientNumberAuth(String patient_number_auth) {
    }

    default void setProviderQuickCode(String providerQuickCode) {
    }

    default void setProviderRegistrationNum(String providerRegistrationNum) {
    }

    default void setProviderRegistrationNumAuth(String providerRegistrationNumAuth) {
    }
}

