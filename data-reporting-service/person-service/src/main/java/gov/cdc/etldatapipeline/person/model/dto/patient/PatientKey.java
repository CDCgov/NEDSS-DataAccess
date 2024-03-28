package gov.cdc.etldatapipeline.person.model.dto.patient;

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
public class PatientKey implements DataRequiredFields {
    @NonNull
    @JsonProperty("patient_uid")
    private Long patientUid;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("patientUid");
    }
}
