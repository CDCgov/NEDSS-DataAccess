package gov.cdc.etldatapipeline.changedata.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumMetadata {
    @Column(name = "ts_ms")
    @JsonProperty("ts_ms")
    private Long ts_ms;
    @Column(name = "op")
    @JsonProperty("op")
    private String op;
}
