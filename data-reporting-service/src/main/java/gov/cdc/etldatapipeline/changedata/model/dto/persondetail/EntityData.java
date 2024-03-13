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
public class EntityData {
    @JsonProperty("entity_uid")
    private Long entityUid;
    private String typeCd;
    private String recordStatusCd;
    private String rootExtensionTxt;
    @JsonProperty("entity_id_seq")
    private Long entityIdSeq;
    @JsonProperty("ASSIGNING_AUTHORITY_CD")
    private String assigningAuthorityCd;

    public <T extends PersonExtendedProps> T updatePerson(T personFull) {
        if(assigningAuthorityCd.equalsIgnoreCase("SSA")) {
            personFull.setSsn(rootExtensionTxt);
        } else if (typeCd.equalsIgnoreCase("PN")) {
            personFull.setPatientNumber(rootExtensionTxt);
            personFull.setPatientNumberAuth(assigningAuthorityCd);
        } else if (typeCd.equalsIgnoreCase("QEC")) {
            personFull.setProviderQuickCode(rootExtensionTxt);
        } else if (typeCd.equalsIgnoreCase("PRN")) {
            personFull.setProviderRegistrationNum(rootExtensionTxt);
            personFull.setProviderRegistrationNumAuth(assigningAuthorityCd);
        }
        return personFull;
    }
}
