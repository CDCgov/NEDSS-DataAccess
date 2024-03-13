package gov.cdc.etldatapipeline.changedata.model.dto.persondetail;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonExtendedProps;
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

    public <T extends PersonExtendedProps> T updatePerson(T personFull) {
        if (useCd.equalsIgnoreCase("WP")) {
            personFull.setPhoneWork(telephoneNbr);
            personFull.setPhoneExtWork(extensionTxt);
        } else if (useCd.equalsIgnoreCase("H")) {
            personFull.setPhoneHome(telephoneNbr);
            personFull.setPhoneExtHome(extensionTxt);
        } else {
            personFull.setPhoneCell(telephoneNbr);
        }
        return personFull;
    }
}