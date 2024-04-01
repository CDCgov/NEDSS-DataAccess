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
public class Email {
    private String emailAddress;
    @JsonProperty("use_cd")
    private String useCd;
    private String cd;
    @JsonProperty("tele_locator_uid")
    private Long teleLocatorUid;

    public <T extends PersonExtendedProps> T updatePerson(T personFull) {
        personFull.setEmail(emailAddress);
        personFull.setEmailElpCd(cd);
        personFull.setEmailElpUseCd(useCd);
        personFull.setEmailTlUid(teleLocatorUid);
        return personFull;
    }
}
