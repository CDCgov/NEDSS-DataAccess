package gov.cdc.etldatapipeline.person.model.dto.provider;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.person.model.dto.DataRequiredFields;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Set;

@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ProviderKey implements DataRequiredFields {
    @NonNull
    private Long providerUid;

    public static ProviderKey build(Provider p) {
        return ProviderKey.builder().providerUid(p.getPersonUid()).build();
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("providerUid");
    }
}
