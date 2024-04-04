package gov.cdc.etldatapipeline.organization.model.dto.org;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.etldatapipeline.organization.utils.DataPostProcessor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrgSp {
    @Id
    @Column(name = "organization_uid")
    private Long organizationUid;
    @Column(name = "description")
    private String description;
    @Column(name = "cd")
    private String cd;
    @Column(name = "electronic_ind")
    private String electronicInd;
    @Column(name = "record_status_cd")
    private String recordStatusCd;
    @Column(name = "record_status_time")
    private String recordStatusTime;
    @Column(name = "status_cd")
    private String statusCd;
    @Column(name = "status_time")
    private String statusTime;
    @Column(name = "local_id")
    private String localId;
    @Column(name = "version_ctrl_nbr")
    private String versionCtrlNbr;
    @Column(name = "edx_ind")
    private String edxInd;
    @Column(name = "stand_ind_class")
    private String standIndClass;
    @Column(name = "add_user_name")
    private String addUserName;
    @Column(name = "last_chg_user_name")
    private String lastChgUserName;
    @Column(name = "add_user_id")
    private Long addUserId;
    @Column(name = "last_chg_user_id")
    private Long lastChgUserId;
    @Column(name = "add_time")
    private String addTime;
    @Column(name = "last_chg_time")
    private String lastChgTime;
    @Column(name = "organization_name")
    private String organizationName;
    @Column(name = "organization_address")
    private String organizationAddress;
    @Column(name = "organization_telephone")
    private String organizationTelephone;
    @Column(name = "organization_fax")
    private String organizationFax;
    @Column(name = "organization_entity_id")
    private String organizationEntityId;


    public OrgReporting processOrgReporting() {
        return postProcessJsonData(new OrgReporting().constructObject(this));
    }

    public OrgElastic processOrgElastic() {
        return postProcessJsonData(new OrgElastic().constructObject(this));
    }

    private <T> T postProcessJsonData(T pf) {
        DataPostProcessor processor = new DataPostProcessor();
        return pf;
    }
}

