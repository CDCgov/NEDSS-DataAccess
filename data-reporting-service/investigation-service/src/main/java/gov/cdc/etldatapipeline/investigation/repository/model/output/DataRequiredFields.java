package gov.cdc.etldatapipeline.investigation.repository.model.output;

import java.beans.Transient;
import java.util.Set;

public interface DataRequiredFields {
    @Transient
    Set<String> getRequiredFields();
}

