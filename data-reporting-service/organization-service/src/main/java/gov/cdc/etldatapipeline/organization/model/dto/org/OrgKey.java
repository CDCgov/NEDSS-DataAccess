package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrgKey implements DataRequiredFields {
    @NonNull
    @JsonProperty("organization_uid")
    private Long orgUID;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("orgUID");
    }
}
