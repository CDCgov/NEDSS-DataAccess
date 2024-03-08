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
public class Phone {
    private String telephoneNbr;
    private String extensionTxt;
    @JsonProperty("use_cd")
    private String useCd;
    private String cd;
    @JsonProperty("tele_locator_uid")
    private Long teleLocatorUid;

    public PatientOP updatePerson(PatientOP patient) {
        if (useCd.equalsIgnoreCase("WP")) {
            patient.setPatientPhoneWork(telephoneNbr);
            patient.setPatientPhoneExtWork(extensionTxt);
        } else if (useCd.equalsIgnoreCase("H")) {
            patient.setPatientPhoneHome(telephoneNbr);
            patient.setPatientPhoneExtHome(extensionTxt);
        } else {
            patient.setPatientPhoneCell(telephoneNbr);
        }
        return patient;
    }
}