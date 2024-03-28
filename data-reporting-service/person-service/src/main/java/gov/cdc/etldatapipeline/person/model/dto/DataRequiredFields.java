package gov.cdc.etldatapipeline.person.model.dto;

import java.beans.Transient;
import java.util.Set;

public interface DataRequiredFields {
    @Transient
    Set<String> getRequiredFields();
}
