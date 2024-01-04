package gov.cdc.etldatapipeline.changedata.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Person extends DebeziumMetadata {
    @Column(name = "person_uid")
    @JsonProperty("person_uid")
    private String personUid;
    @Column(name = "local_id")
    @JsonProperty("local_id")
    private String localId;
    @Column(name = "age_reported")
    @JsonProperty("age_reported")
    private String ageReported;
    @Column(name = "age_reported_unit_cd")
    @JsonProperty("age_reported_unit_cd")
    private String ageReportedUnitCd;
    @Column(name = "birth_gender_cd")
    @JsonProperty("birth_gender_cd")
    private String birthGenderCd;
    @Column(name = "birth_time")
    @JsonProperty("birth_time")
    private String birthTime;
    @Column(name = "curr_sex_cd")
    @JsonProperty("curr_sex_cd")
    private String currSexCd;
    @Column(name = "deceased_ind_cd")
    @JsonProperty("deceased_ind_cd")
    private String deceasedIndCd;
    @Column(name = "add_user_id")
    @JsonProperty("add_user_id")
    private String addUserId;
    @Column(name = "last_chg_user_id")
    @JsonProperty("last_chg_user_id")
    private String lastChgUserId;
    @Column(name = "speaks_english_cd")
    @JsonProperty("speaks_english_cd")
    private String speaksEnglishCd;
    @Column(name = "additional_gender_cd")
    @JsonProperty("additional_gender_cd")
    private String additionalGenderCd;
    @Column(name = "ethnic_unk_reason_cd")
    @JsonProperty("ethnic_unk_reason_cd")
    private String ethnicUnkReasonCd;
    @Column(name = "sex_unk_reason_cd")
    @JsonProperty("sex_unk_reason_cd")
    private String sexUnkReasonCd;
    @Column(name = "preferred_gender_cd")
    @JsonProperty("preferred_gender_cd")
    private String preferredGenderCd;
    @Column(name = "deceased_time")
    @JsonProperty("deceased_time")
    private String deceasedTime;
    @Column(name = "description")
    @JsonProperty("description")
    private String description;
    @Column(name = "electronic_ind")
    @JsonProperty("electronic_ind")
    private String electronicInd;
    @Column(name = "ethnic_group_ind")
    @JsonProperty("ethnic_group_ind")
    private String ethnicGroupInd;
    @Column(name = "marital_status_cd")
    @JsonProperty("marital_status_cd")
    private String maritalStatusCd;
    @Column(name = "person_parent_uid")
    @JsonProperty("person_parent_uid")
    private String personParentUid;
    @Column(name = "last_chg_time")
    @JsonProperty("last_chg_time")
    private String lastChgTime;
    @Column(name = "add_time")
    @JsonProperty("add_time")
    private String addTime;
    @Column(name = "record_status_cd")
    @JsonProperty("record_status_cd")
    private String recordStatusCd;
    @Column(name = "occupation_cd")
    @JsonProperty("occupation_cd")
    private String occupationCd;
    @Column(name = "prim_lang_cd")
    @JsonProperty("prim_lang_cd")
    private String primLangCd;
}
