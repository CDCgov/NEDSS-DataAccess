package gov.cdc.etldatapipeline.changedata.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Organization extends DebeziumMetadata {
    @Column(name = "organization_uid")
    @JsonProperty("organization_uid")
    private String organizationUid;
    @Column(name = "add_reason_cd")
    @JsonProperty("add_reason_cd")
    private String addReasonCd;
    @Column(name = "add_time")
    @JsonProperty("add_time")
    private String addTime;
    @Column(name = "add_user_id")
    @JsonProperty("add_user_id")
    private String addUserId;
    @Column(name = "cd")
    @JsonProperty("cd")
    private String cd;
    @Column(name = "cd_desc_txt")
    @JsonProperty("cd_desc_txt")
    private String cdDescTxt;
    @Column(name = "description")
    @JsonProperty("description")
    private String description;
    @Column(name = "duration_amt")
    @JsonProperty("duration_amt")
    private String durationAmt;
    @Column(name = "duration_unit_cd")
    @JsonProperty("duration_unit_cd")
    private String durationUnitCd;
    @Column(name = "from_time")
    @JsonProperty("from_time")
    private String fromTime;
    @Column(name = "last_chg_reason_cd")
    @JsonProperty("last_chg_reason_cd")
    private String lastChgReasonCd;
    @Column(name = "last_chg_time")
    @JsonProperty("last_chg_time")
    private String lastChgTime;
    @Column(name = "last_chg_user_id")
    @JsonProperty("last_chg_user_id")
    private String lastChgUserId;
    @Column(name = "local_id")
    @JsonProperty("local_id")
    private String localId;
    @Column(name = "record_status_cd")
    @JsonProperty("record_status_cd")
    private String recordStatusCd;
    @Column(name = "record_status_time")
    @JsonProperty("record_status_time")
    private String recordStatusTime;
    @Column(name = "standard_industry_class_cd")
    @JsonProperty("standard_industry_class_cd")
    private String standardIndustryClassCd;
    @Column(name = "standard_industry_desc_txt")
    @JsonProperty("standard_industry_desc_txt")
    private String standardIndustryDescTxt;
    @Column(name = "status_cd")
    @JsonProperty("status_cd")
    private String statusCd;
    @Column(name = "status_time")
    @JsonProperty("status_time")
    private String statusTime;
    @Column(name = "to_time")
    @JsonProperty("to_time")
    private String toTime;
    @Column(name = "user_affiliation_txt")
    @JsonProperty("user_affiliation_txt")
    private String userAffiliationTxt;
    @Column(name = "display_nm")
    @JsonProperty("display_nm")
    private String displayNm;
    @Column(name = "street_addr1")
    @JsonProperty("street_addr1")
    private String streetAddr1;
    @Column(name = "street_addr2")
    @JsonProperty("street_addr2")
    private String streetAddr2;
    @Column(name = "city_cd")
    @JsonProperty("city_cd")
    private String cityCd;
    @Column(name = "city_desc_txt")
    @JsonProperty("city_desc_txt")
    private String cityDescTxt;
    @Column(name = "state_cd")
    @JsonProperty("state_cd")
    private String stateCd;
    @Column(name = "cnty_cd")
    @JsonProperty("cnty_cd")
    private String cntyCd;
    @Column(name = "cntry_cd")
    @JsonProperty("cntry_cd")
    private String cntryCd;
    @Column(name = "zip_cd")
    @JsonProperty("zip_cd")
    private String zipCd;
    @Column(name = "phone_nbr")
    @JsonProperty("phone_nbr")
    private String phoneNbr;
    @Column(name = "phone_cntry_cd")
    @JsonProperty("phone_cntry_cd")
    private String phoneCntryCd;
    @Column(name = "version_ctrl_nbr")
    @JsonProperty("version_ctrl_nbr")
    private String versionCtrlNbr;
    @Column(name = "electronic_ind")
    @JsonProperty("electronic_ind")
    private String electronicInd;
    @Column(name = "edx_ind")
    @JsonProperty("edx_ind")
    private String edxInd;




}
