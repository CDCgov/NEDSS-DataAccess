package gov.cdc.etldatapipeline.person.model.odse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Person extends DebeziumMetadata {
    @Column(name = "person_uid")
    @JsonProperty("person_uid")
    private String personUid;
    @Column(name = "add_reason_cd")
    @JsonProperty("add_reason_cd")
    private String addReasonCd;
    @Column(name = "add_time")
    @JsonProperty("add_time")
    private String addTime;
    @Column(name = "add_user_id")
    @JsonProperty("add_user_id")
    private String addUserId;
    @Column(name = "administrative_gender_cd")
    @JsonProperty("administrative_gender_cd")
    private String administrativeGenderCd;
    @Column(name = "age_calc")
    @JsonProperty("age_calc")
    private String ageCalc;
    @Column(name = "age_calc_time")
    @JsonProperty("age_calc_time")
    private String ageCalcTime;
    @Column(name = "age_calc_unit_cd")
    @JsonProperty("age_calc_unit_cd")
    private String ageCalcUnitCd;
    @Column(name = "age_category_cd")
    @JsonProperty("age_category_cd")
    private String ageCategoryCd;
    @Column(name = "age_reported")
    @JsonProperty("age_reported")
    private String ageReported;
    @Column(name = "age_reported_time")
    @JsonProperty("age_reported_time")
    private String ageReportedTime;
    @Column(name = "age_reported_unit_cd")
    @JsonProperty("age_reported_unit_cd")
    private String ageReportedUnitCd;
    @Column(name = "birth_gender_cd")
    @JsonProperty("birth_gender_cd")
    private String birthGenderCd;
    @Column(name = "birth_order_nbr")
    @JsonProperty("birth_order_nbr")
    private String birthOrderNbr;
    @Column(name = "birth_time")
    @JsonProperty("birth_time")
    private String birthTime;
    @Column(name = "birth_time_calc")
    @JsonProperty("birth_time_calc")
    private String birthTimeCalc;
    @Column(name = "cd")
    @JsonProperty("cd")
    private String cd;
    @Column(name = "cd_desc_txt")
    @JsonProperty("cd_desc_txt")
    private String cdDescTxt;
    @Column(name = "curr_sex_cd")
    @JsonProperty("curr_sex_cd")
    private String currSexCd;
    @Column(name = "deceased_ind_cd")
    @JsonProperty("deceased_ind_cd")
    private String deceasedIndCd;
    @Column(name = "deceased_time")
    @JsonProperty("deceased_time")
    private String deceasedTime;
    @Column(name = "description")
    @JsonProperty("description")
    private String description;
    @Column(name = "education_level_cd")
    @JsonProperty("education_level_cd")
    private String educationLevelCd;
    @Column(name = "education_level_desc_txt")
    @JsonProperty("education_level_desc_txt")
    private String educationLevelDescTxt;
    @Column(name = "ethnic_group_ind")
    @JsonProperty("ethnic_group_ind")
    private String ethnicGroupInd;
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
    @Column(name = "marital_status_cd")
    @JsonProperty("marital_status_cd")
    private String maritalStatusCd;
    @Column(name = "marital_status_desc_txt")
    @JsonProperty("marital_status_desc_txt")
    private String maritalStatusDescTxt;
    @Column(name = "occupation_cd")
    @JsonProperty("occupation_cd")
    private String occupationCd;
    @Column(name = "preferred_gender_cd")
    @JsonProperty("preferred_gender_cd")
    private String preferredGenderCd;
    @Column(name = "prim_lang_cd")
    @JsonProperty("prim_lang_cd")
    private String primLangCd;
    @Column(name = "prim_lang_desc_txt")
    @JsonProperty("prim_lang_desc_txt")
    private String primLangDescTxt;
    @Column(name = "record_status_cd")
    @JsonProperty("record_status_cd")
    private String recordStatusCd;
    @Column(name = "record_status_time")
    @JsonProperty("record_status_time")
    private String recordStatusTime;
    @Column(name = "status_cd")
    @JsonProperty("status_cd")
    private String statusCd;
    @Column(name = "status_time")
    @JsonProperty("status_time")
    private String statusTime;
    @Column(name = "survived_ind_cd")
    @JsonProperty("survived_ind_cd")
    private String survivedIndCd;
    @Column(name = "user_affiliation_txt")
    @JsonProperty("user_affiliation_txt")
    private String userAffiliationTxt;
    @Column(name = "dl_num")
    @JsonProperty("dl_num")
    private String dlNum;
    @Column(name = "dl_state_cd")
    @JsonProperty("dl_state_cd")
    private String dlStateCd;
    @Column(name = "race_cd")
    @JsonProperty("race_cd")
    private String raceCd;
    @Column(name = "race_seq_nbr")
    @JsonProperty("race_seq_nbr")
    private String raceSeqNbr;
    @Column(name = "race_category_cd")
    @JsonProperty("race_category_cd")
    private String raceCategoryCd;
    @Column(name = "electronic_ind")
    @JsonProperty("electronic_ind")
    private String electronicInd;
    @Column(name = "person_parent_uid")
    @JsonProperty("person_parent_uid")
    private String personParentUid;
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
}
