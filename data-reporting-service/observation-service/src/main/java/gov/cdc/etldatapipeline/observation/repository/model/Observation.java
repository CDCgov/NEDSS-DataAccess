package gov.cdc.etldatapipeline.observation.repository.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Entity
@Getter
@Setter
public class Observation {

    @Column(name = "act_uid")
    private Long actUid;

    @Column(name = "class_cd")
    private String classCd;

    @Column(name = "mood_cd")
    private String moodCd;

    @Id
    @Column(name = "observation_uid")
    private Long id;

    @Column(name = "obs_domain_cd_st_1")
    private String obsDomainCdSt1;

    @Column(name = "cd_desc_txt")
    private String cdDescText;

    @Column(name = "record_status_cd")
    private String recordStatusCd;

    @Column(name = "program_jurisdiction_oid")
    private Long programJurisdictionOid;

    @Column(name = "prog_area_cd")
    private String progAreaCd;

    @Column(name = "jurisdiction_cd")
    private String jurisdictionCd;

    @Column(name = "pregnant_ind_cd")
    private String pregnantIndCd;

    @Column(name = "local_id")
    private String localId;

    @Column(name = "activity_to_time")
    private Instant activityToTime;

    @Column(name = "effective_from_time")
    private Instant effectiveFromTime;

    @Column(name = "rpt_to_state_time")
    private Instant rptToStateTime;

    @Column(name = "electronic_ind")
    private Character electronicInd;

    @Column(name = "version_ctrl_nbr")
    private Short versionCtrlNbr;

    @Column(name = "add_user_id")
    private Long addUserId;

    @Column(name = "add_user_name")
    private String addUserName;

    @Column(name = "last_chg_user_id")
    private Long lastChgUserId;

    @Column(name = "last_chg_user_name")
    private String lastChgUserName;

    @Column(name = "add_time")
    private Instant addTime;

    @Column(name = "last_chg_time")
    private Instant lastChgTime;

//    @Column(name = "observation_add_time")
//    private Instant observationAddTime;
//
//    @Column(name = "observation_last_chg_time")
//    private Instant observationLastChgTime;

    @Column(name = "person_participations")
    private String personParticipations;

    @Column(name = "organization_participations")
    private String organizationParticipations;

    @Column(name = "material_participations")
    private String materialParticipations;

    @Column(name = "followup_observations")
    private String followupObservations;

    @Column(name = "act_ids")
    private String actIds;

}
