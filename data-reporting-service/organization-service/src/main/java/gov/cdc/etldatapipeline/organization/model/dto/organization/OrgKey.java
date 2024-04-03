package gov.cdc.etldatapipeline.organization.model.dto.organization;

import com.fasterxml.jackson.annotation.JsonProperty;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class OrgKey implements DataRequiredFields {
    @NonNull
    @JsonProperty("organization_uid")
    private Long orgUID;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("patientUid");
    }
}
