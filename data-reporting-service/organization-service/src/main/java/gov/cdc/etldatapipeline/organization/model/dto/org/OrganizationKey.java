package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.commonutil.model.DataRequiredFields;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Set;

@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrganizationKey implements DataRequiredFields {
    @NonNull
    @JsonProperty("organization_uid")
    private Long organizationUid;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("organizationUid");
    }
}
