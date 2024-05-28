package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;


@Data
@Entity
@Table(name = "nrt_page_case_answer")
public class InvestigationCaseAnswer {
    @JsonProperty("nbs_case_answer_uid")
    @Column(name = "nbs_case_answer_uid")
    private String nbsCaseAnswerUid;

    @JsonProperty("nbs_ui_metadata_uid")
    @Column(name = "nbs_ui_metadata_uid")
    private String nbsUiMetadataUid;

    @JsonProperty("nbs_rdb_metadata_uid")
    @Column(name = "nbs_rdb_metadata_uid")
    private String nbsRdbMetadataUid;

    @JsonProperty("rdb_table_nm")
    @Column(name = "rdb_table_nm")
    private String rdbTableNm;

    @JsonProperty("rdb_column_nm")
    @Column(name = "rdb_column_nm")
    private String rdbColumnNm;

    @JsonProperty("code_set_group_id")
    @Column(name = "code_set_group_id")
    private String codeSetGroupId;

    @JsonProperty("answer_txt")
    @Column(name = "answer_txt")
    private String answerTxt;

//    @JsonProperty("page_case_uid_text")
//    @Column(name = "page_case_uid_text")
//    private String pageCaseUidText;

    @Id
    @JsonProperty("act_uid")
    @Column(name = "act_uid")
    private String actUid;

    @JsonProperty("record_status_cd")
    @Column(name = "record_status_cd")
    private String recordStatusCd;

    @JsonProperty("nbs_question_uid")
    @Column(name = "nbs_question_uid")
    private String nbsQuestionUid;

    @JsonProperty("investigation_form_cd")
    @Column(name = "investigation_form_cd")
    private String investigationFormCd;

    @JsonProperty("unit_value")
    @Column(name = "unit_value")
    private String unitValue;

    @JsonProperty("question_identifier")
    @Column(name = "question_identifier")
    private String questionIdentifier;

    @JsonProperty("data_location")
    @Column(name = "data_location")
    private String dataLocation;

    @JsonProperty("answer_group_seq_nbr")
    @Column(name = "answer_group_seq_nbr")
    private String answerGroupSeqNbr;

    @JsonProperty("question_label")
    @Column(name = "question_label")
    private String questionLabel;

    @JsonProperty("other_value_ind_cd")
    @Column(name = "other_value_ind_cd")
    private String otherValueIndCd;

    @JsonProperty("unit_type_cd")
    @Column(name = "unit_type_cd")
    private String unitTypeCd;

    @JsonProperty("mask")
    @Column(name = "mask")
    private String mask;

    @JsonProperty("question_group_seq_nbr")
    @Column(name = "question_group_seq_nbr")
    private String questionGroupSeqNbr;

    @JsonProperty("data_type")
    @Column(name = "data_type")
    private String dataType;

    @JsonProperty("last_chg_time")
    @Column(name = "last_chg_time")
    private String lastChgTime;
}