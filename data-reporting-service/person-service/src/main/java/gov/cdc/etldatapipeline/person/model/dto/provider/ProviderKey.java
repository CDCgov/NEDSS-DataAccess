package gov.cdc.etldatapipeline.person.model.dto.provider;

import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.person.model.dto.DataRequiredFields;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class ProviderKey implements DataRequiredFields {
    @NonNull
    @JsonProperty("provider_uid")
    private Long providerUid;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("providerUid");
    }
}
