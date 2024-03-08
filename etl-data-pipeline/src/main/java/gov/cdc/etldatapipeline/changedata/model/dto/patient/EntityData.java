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
public class EntityData {
    @JsonProperty("entity_uid")
    private Long entityUid;
    private String typeCd;
    private String recordStatusCd;
    private String rootExtensionTxt;
    @JsonProperty("entityIdSeq")
    private Long entityIdSeq;
    @JsonProperty("ASSIGNING_AUTHORITY_CD")
    private String assigningAuthorityCd;

    public PatientOP updatePerson(PatientOP patient) {
        patient.setPatientSsn(rootExtensionTxt);
        return patient;
    }
}
