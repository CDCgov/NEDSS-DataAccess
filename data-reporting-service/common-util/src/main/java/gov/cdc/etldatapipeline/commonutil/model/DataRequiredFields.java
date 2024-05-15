package gov.cdc.etldatapipeline.commonutil.model;

import java.beans.Transient;
import java.util.Set;

public interface DataRequiredFields {
    @Transient
    Set<String> getRequiredFields();
}
