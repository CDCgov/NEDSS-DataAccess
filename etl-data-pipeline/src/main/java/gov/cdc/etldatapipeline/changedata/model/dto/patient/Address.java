package gov.cdc.etldatapipeline.changedata.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.changedata.model.dto.PatientOP;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Address {
    private String streetAddr1;
    private String streetAddr2;
    private String city;
    private String zip;
    private String cntyCd;
    private String state;
    private String cntryCd;
    @JsonProperty("state_desc")
    private String stateDesc;
    private String county;
    @JsonProperty("within_city_limits_ind")
    private String withinCityLimitsInd;
    @JsonProperty("home_country")
    private String homeCountry;
    @JsonProperty("birth_country")
    private String birthCountry;
    @JsonProperty("USE_CD")
    private String useCd;
    private String cd;
    @JsonProperty("postal_locator_uid")
    private Long postalLocatorUid;

    public PatientOP updatePerson(PatientOP patient) {
        patient.setPatientStreetAddress1(streetAddr1);
        patient.setPatientStreetAddress2(streetAddr2);
        patient.setPatientCity(city);
        patient.setPatientZip(zip);
        patient.setPatientCountyCode(cntyCd);
        patient.setPatientCounty(county);
        patient.setPatientStateCode(state);
        patient.setPatientState(stateDesc);
        patient.setPatientCountryCode(cntryCd);
        patient.setPatientCountry(homeCountry);
        patient.setPatientBirthCountry(birthCountry);
        return patient;
    }
}
