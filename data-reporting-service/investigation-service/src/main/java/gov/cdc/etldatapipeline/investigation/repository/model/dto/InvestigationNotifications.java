package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import jakarta.persistence.Column;
import lombok.Data;

@Data
public class InvestigationNotifications {
    @Column(name = "source_act_uid")
    private Long sourceActUid;

    @Column(name = "public_health_case_uid")
    private Long publicHealthCaseUid;

    @Column(name = "source_class_cd")
    private String sourceClassCd;

    @Column(name = "target_class_cd")
    private String targetClassCd;

    @Column(name = "act_type_cd")
    private String actTypeCd;

    @Column(name = "status_cd")
    private String statusCd;

    @Column(name = "cd")
    private String cd;

    @Column(name = "notification_uid")
    private Long notificationUid;

    @Column(name = "prog_area_cd")
    private String progAreaCd;

    @Column(name = "program_jurisdiction_oid")
    private Long programJurisdictionOid;

    @Column(name = "jurisdiction_cd")
    private String jurisdictionCd;

    @Column(name = "record_status_time")
    private String recordStatusTime;

    @Column(name = "status_time")
    private String statusTime;

    @Column(name = "rpt_sent_time")
    private String rptSentTime;

    @Column(name = "notif_status")
    private String notifStatus;

    @Column(name = "notif_local_id")
    private String notifLocalId;

    @Column(name = "notif_comments")
    private String notifComments;

    @Column(name = "notif_add_time")
    private String notifAddTime;

    @Column(name = "notif_add_user_id")
    private Long notifAddUserId;

    @Column(name = "notif_add_user_name")
    private String notifAddUserName;

    @Column(name = "notif_last_chg_user_id")
    private String notifLastChgUserId;

    @Column(name = "notif_last_chg_user_name")
    private String notifLastChgUserName;

    @Column(name = "notif_last_chg_time")
    private String notifLastChgTime;

    @Column(name = "local_patient_id")
    private String localPatientId;

    @Column(name = "local_patient_uid")
    private String localPatientUid;
}
