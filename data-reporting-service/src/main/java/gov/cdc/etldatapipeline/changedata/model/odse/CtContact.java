package gov.cdc.etldatapipeline.changedata.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CtContact extends DebeziumMetadata {
    @Column(name = "contact_entity_uid")
    @JsonProperty("contact_entity_uid")
    private String contactEntityUid;
    @Column(name = "relationship_cd")
    @JsonProperty("relationship_cd")
    private String relationshipCd;
    @Column(name = "subject_entity_uid")
    @JsonProperty("subject_entity_uid")
    private String subjectEntityUid;
}
