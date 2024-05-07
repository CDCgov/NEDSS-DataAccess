package gov.cdc.etldatapipeline.person.model.dto.DataProps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEnvelope<T> {
    private DataSchema schema;
    private T payload;
}