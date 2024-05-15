package gov.cdc.etldatapipeline.person.model.dto.patient;

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
public class PatientKey implements DataRequiredFields {
    @NonNull
    private Long patientUid;

    public static PatientKey build(PatientSp p) {
        return PatientKey.builder().patientUid(p.getPersonUid()).build();
    }

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("patientUid");
    }
}
