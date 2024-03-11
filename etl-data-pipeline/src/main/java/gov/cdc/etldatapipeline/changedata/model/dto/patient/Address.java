package gov.cdc.etldatapipeline.changedata.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonOp;
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

    public PersonOp updatePerson(PersonOp personOp) {
        personOp.setStreetAddress1(streetAddr1);
        personOp.setStreetAddress2(streetAddr2);
        personOp.setCity(city);
        personOp.setZip(zip);
        personOp.setCountyCode(cntyCd);
        personOp.setCounty(county);
        personOp.setStateCode(state);
        personOp.setState(stateDesc);
        personOp.setCountryCode(cntryCd);
        personOp.setCountry(homeCountry);
        personOp.setBirthCountry(birthCountry);
        return personOp;
    }
}
