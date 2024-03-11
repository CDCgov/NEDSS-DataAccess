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
public class Phone {
    private String telephoneNbr;
    private String extensionTxt;
    @JsonProperty("use_cd")
    private String useCd;
    private String cd;
    @JsonProperty("tele_locator_uid")
    private Long teleLocatorUid;

    public PersonOp updatePerson(PersonOp personOp) {
        if (useCd.equalsIgnoreCase("WP")) {
            personOp.setPhoneWork(telephoneNbr);
            personOp.setPhoneExtWork(extensionTxt);
        } else if (useCd.equalsIgnoreCase("H")) {
            personOp.setPhoneHome(telephoneNbr);
            personOp.setPhoneExtHome(extensionTxt);
        } else {
            personOp.setPhoneCell(telephoneNbr);
        }
        return personOp;
    }
}