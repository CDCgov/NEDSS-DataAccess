package gov.cdc.etldatapipeline.changedata.model.dto.persondetail;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonFull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Email {
    private String emailAddress;
    @JsonProperty("use_cd")
    private String useCd;
    private String cd;
    @JsonProperty("tele_locator_uid")
    private Long teleLocatorUid;

    public PersonFull updatePerson(PersonFull personFull) {
        personFull.setEmail(emailAddress);
        return personFull;
    }
}
