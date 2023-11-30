package gov.cdc.etldatapipeline.changedata.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NbsPage {
    private Integer nbs_page_uid;
    private Integer wa_template_uid;
    private String form_cd;
    private String desc_txt;
    private String jsp_payload;
    private String datamart_nm;
    private Integer last_chg_user_id;
    private String bus_obj_type;
}
