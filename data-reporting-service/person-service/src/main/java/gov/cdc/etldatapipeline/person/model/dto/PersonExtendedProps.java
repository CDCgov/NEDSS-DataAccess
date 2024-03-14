package gov.cdc.etldatapipeline.person.model.dto;


public interface PersonExtendedProps {
    void setFirstNm(String firstNm);

    void setMiddleNm(String middleNm);

    void setLastNm(String lastNm);

    void setNmSuffix(String nmSuffix);

    void setStreetAddress1(String streetAddress1);

    void setStreetAddress2(String streetAddress2);

    void setCity(String city);

    void setState(String state);

    void setStateCode(String stateCode);

    void setZip(String zip);

    void setCounty(String county);

    void setCountyCode(String countyCode);

    void setCountry(String country);

    void setCountryCode(String countryCode);

    void setBirthCountry(String birthCountry);

    void setPhoneWork(String phoneWork);

    void setPhoneExtWork(String phoneExtWork);

    void setPhoneHome(String phoneHome);

    void setPhoneExtHome(String phoneExtHome);

    void setPhoneCell(String phoneCell);

    void setEmail(String email);

    void setSsn(String ssn);

    void setAddedBy(Long addedBy);

    void setLastChangedBy(Long lastChangedBy);

    void setRaceCd(String raceCd);

    void setRaceCategory(String raceCategory);

    void setRaceDesc(String raceDesc);

    void setPatientNumber(String patientNumber);

    void setPatientNumberAuth(String patientNumberAuth);

    void setProviderQuickCode(String providerQuickCode);

    void setProviderRegistrationNum(String providerRegistrationNum);

    void setProviderRegistrationNumAuth(String providerRegistrationNumAuth);
}

