package gov.cdc.etldatapipeline.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.etldatapipeline.changedata.model.odse.CtContact;
import gov.cdc.etldatapipeline.changedata.model.odse.Participation;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "InitPatient")
public class InitPatient {
    @Id
    @Column(name = "patient_uid")
    private String personUid;
    @Column(name = "patient_local_id")
    private String localId;
    @Column(name = "age_reported")
    private String ageReported;
    @Column(name = "age_reported_unit_cd")
    private String ageReportedUnitCd;
    @Column(name = "birth_gender_cd")
    private String birthGenderCd;
    @Column(name = "patient dob")
    private String birthTime;
    @Column(name = "curr_sex_cd")
    private String currSexCd;
    @Column(name = "deceased_ind_cd")
    private String deceasedIndCd;
    @Column(name = "add_user_id")
    private String addUserId;
    @Column(name = "last_chg_user_id")
    private String lastChgUserId;
    @Column(name = "speaks_english_cd")
    private String speaksEnglishCd;
    @Column(name = "patient_addl_gender_info")
    private String additionalGenderCd;
    @Column(name = "ethnic_unk_reason_cd")
    private String ethnicUnkReasonCd;
    @Column(name = "sex_unk_reason_cd")
    private String sexUnkReasonCd;
    @Column(name = "preferred_gender_cd")
    private String preferredGenderCd;
    @Column(name = "patient_deceased_date")
    private String deceasedTime;
    @Column(name = "description")
    private String description;
    @Column(name = "patient_entry_method")
    private String electronicInd;
    @Column(name = "ethnic_group_ind")
    private String ethnicGroupInd;
    @Column(name = "marital_status_cd")
    private String maritalStatusCd;
    @Column(name = "patient_mpr_uid")
    private String personParentUid;
    @Column(name = "patient_last_change_time")
    private String lastChgTime;
    @Column(name = "patient_add_time")
    private String addTime;
    @Column(name = "patient_record_status")
    private String recordStatusCd;
    @Column(name = "occupation_cd")
    private String occupationCd;
    @Column(name = "prim_lang_cd")
    private String primLangCd;
    @Column(name = "patient_event_uid")
    private String patientEventUid;
    @Column(name = "patient_event_type")
    private String patientEventType;

    public InitPatient constructPatient(Person p) {
        this.personUid = p.getPersonUid();
        this.localId = p.getLocalId();
        this.ageReported = p.getAgeReported();
        this.ageReportedUnitCd = p.getAgeReportedUnitCd();
        this.birthGenderCd = p.getBirthGenderCd();
        this.birthTime = p.getBirthTime();
        this.currSexCd = p.getCurrSexCd();
        this.deceasedIndCd = p.getDeceasedIndCd();
        this.addUserId = p.getAddUserId();
        this.lastChgUserId = p.getLastChgUserId();
        this.speaksEnglishCd = p.getSpeaksEnglishCd();
        this.additionalGenderCd = p.getAdditionalGenderCd();
        this.ethnicUnkReasonCd = p.getEthnicUnkReasonCd();
        this.sexUnkReasonCd = p.getSexUnkReasonCd();
        this.preferredGenderCd = p.getPreferredGenderCd();
        this.deceasedTime = p.getDeceasedTime();
        this.description = p.getDescription();
        this.electronicInd = p.getElectronicInd();
        this.ethnicGroupInd = p.getEthnicGroupInd();
        this.maritalStatusCd = p.getMaritalStatusCd();
        this.personParentUid = p.getPersonParentUid();
        this.lastChgTime = p.getLastChgTime();
        this.addTime = p.getAddTime();
        this.recordStatusCd = p.getRecordStatusCd();
        this.occupationCd = p.getOccupationCd();
        this.primLangCd = p.getPrimLangCd();
        return this;
    }

    public InitPatient constructPatient(Person p, Participation part) {
        InitPatient patient = constructPatient(p);
        patient.patientEventUid = part.getActUid();
        patient.patientEventType = part.getTypeCd();
        return patient;
    }

    public InitPatient constructPatient(Person p, CtContact ctContact) {
        InitPatient patient = constructPatient(p);
        patient.patientEventUid = ctContact.getContactEntityUid();
        patient.patientEventType = ctContact.getRelationshipCd();
        return patient;
    }
}
