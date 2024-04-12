package gov.cdc.etldatapipeline.person.model.avro;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Avro schema class
 *
 * @param <T> Data payload
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEnvelope<T> {
    private DataSchema schema;
    private T payload;
}