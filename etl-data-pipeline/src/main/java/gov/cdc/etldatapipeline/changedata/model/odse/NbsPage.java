package gov.cdc.etldatapipeline.changedata.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "s_nbs_page")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
public class NbsPage extends DebeziumMetadata {
    @Id
    private Integer nbs_page_uid;
    private Integer wa_template_uid;
    private String form_cd;
    private String desc_txt;
    private byte[] jsp_payload;
    private String datamart_nm;
    private Integer last_chg_user_id;
    private String bus_obj_type;
}
