package gov.cdc.etldatapipeline.person.model.dto.persondetail;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
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
    @JsonProperty("address_comments")
    private String addressComments;

    public <T extends PersonExtendedProps> T updatePerson(T personFull) {
        personFull.setStreetAddress1(streetAddr1);
        personFull.setStreetAddress2(streetAddr2);
        personFull.setCity(city);
        personFull.setZip(zip);
        personFull.setCountyCode(cntyCd);
        personFull.setCounty(county);
        personFull.setStateCode(state);
        personFull.setState(stateDesc);
        personFull.setCountryCode(cntryCd);
        personFull.setHomeCountry(homeCountry);
        personFull.setBirthCountry(birthCountry);
        personFull.setAddressComments(addressComments);
        return personFull;
    }
}
