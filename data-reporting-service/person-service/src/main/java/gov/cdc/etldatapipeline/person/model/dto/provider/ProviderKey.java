package gov.cdc.etldatapipeline.person.model.dto.provider;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class ProviderKey {
    @NonNull
    private Long personUid;
}
