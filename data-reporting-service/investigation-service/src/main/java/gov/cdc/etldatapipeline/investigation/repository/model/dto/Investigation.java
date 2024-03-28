package gov.cdc.etldatapipeline.investigation.repository.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Entity
@Getter
@Setter
public class Investigation {

    @Id
    @Column(name = "public_health_case_uid")
    private Long publicHealthCaseUid;

    @Column(name = "program_jurisdiction_oid")
    private Long programJurisdictionOid;

    @Column(name = "jurisdiction_code")
    private String jurisdictionCode;

    @Column(name = "jurisdiction_code_desc_txt")
    private String jurisdictionCodeDescTxt;

    @Column(name = "mood_cd")
    private String moodCd;

    @Column(name = "class_cd")
    private String classCd;

    @Column(name = "case_type_cd")
    private String caseTypeCd;

    @Column(name = "case_class_cd")
    private String caseClassCd;

    @Column(name = "outbreak_name")
    private String outbreakName;

    @Column(name = "cd")
    private String cd;

    @Column(name = "cd_desc_txt")
    private String cdDescTxt;

    @Column(name = "prog_area_cd")
    private String progAreaCd;

    @Column(name = "jurisdiction_cd")
    private String jurisdictionCd;

    @Column(name = "pregnant_ind_cd")
    private String pregnantIndCd;

    @Column(name = "local_id")
    private String localId;

    @Column(name = "rpt_form_cmplt_time")
    private Instant rptFormCmpltTime;

    @Column(name = "activity_to_time")
    private Instant activityToTime;

    @Column(name = "activity_from_time")
    private Instant activityFromTime;

    @Column(name = "add_user_id")
    private Long addUserId;

    @Column(name = "add_user_name")
    private String addUserName;

    @Column(name = "add_time")
    private Instant addTime;

    @Column(name = "last_chg_user_id")
    private Long lastChgUserId;

    @Column(name = "last_chg_user_name")
    private String lastChgUserName;

    @Column(name = "last_chg_time")
    private Instant lastChgTime;

    @Column(name = "curr_process_state_cd")
    private String currProcessStateCd;

    @Column(name = "investigation_status_cd")
    private String investigationStatusCd;

    @Column(name = "record_status_cd")
    private String recordStatusCd;

    @Column(name = "notification_local_id")
    private Long notificationLocalId;

    @Column(name = "notification_add_time")
    private Instant notification_add_time;

    @Column(name = "notification_record_status_cd")
    private String notification_record_status_cd;

    @Column(name = "notification_last_chg_time")
    private Instant notificationLastChgTime;

    @Column(name = "act_ids")
    private String actIds;

    @Column(name = "observation_notification_ids")
    private String observationNotificationIds;

    @Column(name = "person_participations")
    private String personParticipations;

    @Column(name = "organization_participations")
    private String organizationParticipations;

    @Column(name = "investigation_confirmation_method")
    private String investigationConfirmationMethod;
}
