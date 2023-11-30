package gov.cdc.etldatapipeline.changedata.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NbsPageId {
    private Integer nbs_page_uid;
}
