package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Set;

@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrgKey implements DataRequiredFields {
    @NonNull
    @JsonProperty("organization_uid")
    private Long orgUID;

    public static OrgKey build(OrgSp p) {
        return OrgKey.builder().orgUID(p.getOrganizationUid()).build();
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("orgUID");
    }
}
