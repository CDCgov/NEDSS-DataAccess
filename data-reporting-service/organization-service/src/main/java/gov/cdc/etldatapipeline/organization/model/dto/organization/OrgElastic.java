package gov.cdc.etldatapipeline.organization.model.dto.organization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Set;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OrgElastic implements DataRequiredFields {

    public OrgElastic constructObject(OrgSp org) {
        OrgElastic orgSp = new OrgElastic();
        return orgSp;
    }

    @Override
    public Set<String> getRequiredFields() {
        return null;
    }
}
