package gov.cdc.etldatapipeline.changedata.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Participation extends DebeziumMetadata {
    @Column(name = "act_uid")
    @JsonProperty("act_uid")
    private String actUid;
    @Column(name = "type_cd")
    @JsonProperty("type_cd")
    private String typeCd;
    @Column(name = "subject_entity_uid")
    @JsonProperty("subject_entity_uid")
    private String subjectEntityUid;
}
