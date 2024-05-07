package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

import java.time.Instant;

@Entity
@Data
public class Investigation {

    @Id
    @Column(name = "public_health_case_uid")
    private Long publicHealthCaseUid;

    @Column(name = "program_jurisdiction_oid")
    private Long programJurisdictionOid;

    @Column(name = "jurisdiction_code")
    private String jurisdictionCode;

    @Column(name = "jurisdiction_nm")
    private String jurisdictionNm;

    @Column(name = "mood_cd")
    private String moodCd;

    @Column(name = "class_cd")
    private String classCd;

    @Column(name = "case_type_cd")
    private String caseTypeCd;

    @Column(name = "case_class_cd")
    private String caseClassCd;

    @Column(name = "inv_case_status")
    private String invCaseStatus;

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

    @Column(name = "pregnant_ind")
    private String pregnantInd;

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

    @Column(name = "curr_process_state")
    private String currProcessState;

    @Column(name = "investigation_status_cd")
    private String investigationStatusCd;

    @Column(name = "investigation_status")
    private String investigationStatus;

    @Column(name = "record_status_cd")
    private String recordStatusCd;

    @Column(name = "shared_ind")
    private String sharedInd;

    @Column(name = "txt")
    private String txt;

    @Column(name = "effective_from_time")
    private Instant effectiveFromTime;

    @Column(name = "effective_to_time")
    private Instant effectiveToTime;

    @Column(name = "rpt_source_cd")
    private String rptSourceCd;

    @Column(name = "rpt_src_cd_desc")
    private String rptSrcCdDesc;

    @Column(name = "rpt_to_county_time")
    private Instant rptToCountyTime;

    @Column(name = "rpt_to_state_time")
    private Instant rptToStateTime;

    @Column(name = "mmwr_week")
    private String mmwrWeek;

    @Column(name = "mmwr_year")
    private String mmwrYear;

    @Column(name = "disease_imported_cd")
    private String diseaseImportedCd;

    @Column(name = "disease_imported_ind")
    private String diseaseImportedInd;

    @Column(name = "imported_country_cd")
    private String importedCountryCd;

    @Column(name = "imported_state_cd")
    private String importedStateCd;

    @Column(name = "imported_county_cd")
    private String importedCountyCd;

    @Column(name = "imported_from_country")
    private String importedFromCountry;

    @Column(name = "imported_from_state")
    private String importedFromState;

    @Column(name = "imported_from_county")
    private String importedFromCounty;

    @Column(name = "imported_city_desc_txt")
    private String importedCityDescTxt;

    @Column(name = "diagnosis_time")
    private Instant diagnosisTime;

    @Column(name = "hospitalized_admin_time")
    private Instant hospitalizedAdminTime;

    @Column(name = "hospitalized_discharge_time")
    private Instant hospitalizedDischargeTime;

    @Column(name = "hospitalized_duration_amt")
    private Long hospitalizedDurationAmt;

    @Column(name = "outbreak_ind")
    private String outbreakInd;

    @Column(name = "outbreak_ind_val")
    private String outbreakIndVal;

    @Column(name = "hospitalized_ind_cd")
    private String hospitalizedIndCd;

    @Column(name = "hospitalized_ind")
    private String hospitalizedInd;

    @Column(name = "transmission_mode_cd")
    private String transmissionModeCd;

    @Column(name = "transmission_mode")
    private String transmissionMode;

    @Column(name = "outcome_cd")
    private String outcomeCd;

    @Column(name = "die_frm_this_illness_ind")
    private String dieFrmThisIllnessInd;

    @Column(name = "day_care_ind_cd")
    private String dayCareIndCd;

    @Column(name = "day_care_ind")
    private String dayCareInd;

    @Column(name = "food_handler_ind_cd")
    private String foodHandlerIndCd;

    @Column(name = "food_handler_ind")
    private String foodHandlerInd;

    @Column(name = "deceased_time")
    private Instant deceasedTime;

    @Column(name = "pat_age_at_onset")
    private String patAgeAtOnset;

    @Column(name = "pat_age_at_onset_unit_cd")
    private String patAgeAtOnsetUnitCd;

    @Column(name = "pat_age_at_onset_unit")
    private String patAgeAtOnsetUnit;

    @Column(name = "detection_method_cd")
    private String detectionMethodCd;

    @Column(name = "detection_method_desc_txt")
    private String detectionMethodDescTxt;

    @Column(name = "priority_cd")
    private String priorityCd;

    @Column(name = "contact_inv_priority")
    private String contactInvPriority;

    @Column(name = "contact_inv_status_cd")
    private String contactInvStatusCd;

    @Column(name = "contact_inv_status")
    private String contactInvStatus;

    @Column(name = "investigator_assigned_time")
    private Instant investigatorAssignedTime;

    @Column(name = "effective_duration_amt")
    private String effectiveDurationAmt;

    @Column(name = "effective_duration_unit_cd")
    private String effectiveDurationUnitCd;

    @Column(name = "illness_duration_unit")
    private String illnessDurationUnit;

    @Column(name = "infectious_from_date")
    private Instant infectiousFromDate;

    @Column(name = "infectious_to_date")
    private Instant infectiousToDate;

    @Column(name = "referral_basis_cd")
    private String referralBasisCd;

    @Column(name = "referral_basis")
    private String referralBasis;

    @Column(name = "inv_priority_cd")
    private String invPriorityCd;

    @Column(name = "coinfection_id")
    private String coinfectionId;

    @Column(name = "contact_inv_txt")
    private String contactInvTxt;

    @Column(name = "program_area_description")
    private String programAreaDescription;

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
