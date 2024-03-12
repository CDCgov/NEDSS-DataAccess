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

    public PersonFull updatePerson(PersonFull personFull) {
        personFull.setSsn(rootExtensionTxt);
        return personFull;
    }
}
