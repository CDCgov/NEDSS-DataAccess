package gov.cdc.etldatapipeline.person.model.dto.patient;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class PatientKey {
    @NonNull
    private Long personUid;
}
