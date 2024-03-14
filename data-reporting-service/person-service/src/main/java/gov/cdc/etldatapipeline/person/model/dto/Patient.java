package gov.cdc.etldatapipeline.person.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import gov.cdc.etldatapipeline.person.utils.DataPostProcessor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Patient {
    @Id
    @Column(name = "person_uid")
    private Long personUid;
    @Column(name = "person_parent_uid")
    private Long personParentUid;
    @Column(name = "description")
    private String description;
    @Column(name = "add_time")
    private String addTime;
    @Column(name = "age_reported")
    private String ageReported;
    @Column(name = "age_reported_unit_cd")
    private String ageReportedUnitCd;
    @Column(name = "first_nm")
    private String firstNm;
    @Column(name = "middle_nm")
    private String middleNm;
    @Column(name = "last_nm")
    private String lastNm;
    @Column(name = "nm_suffix")
    private String nmSuffix;
    @Column(name = "as_of_date_admin")
    private String asOfDateAdmin;
    @Column(name = "as_of_date_ethnicity")
    private String asOfDateEthnicity;
    @Column(name = "as_of_date_general")
    private String asOfDateGeneral;
    @Column(name = "as_of_date_morbidity")
    private String asOfDateMorbidity;
    @Column(name = "as_of_date_sex")
    private String asOfDateSex;
    @Column(name = "birth_time")
    private String birthTime;
    @Column(name = "birth_time_calc")
    private String birthTimeCalc;
    @Column(name = "cd")
    private String cd;
    @Column(name = "currSexCd")
    private String currSexCd;
    @Column(name = "deceasedIndCd")
    private String deceasedIndCd;
    @Column(name = "electronic_ind")
    private String electronicInd;
    @Column(name = "ethnic_group_ind")
    private String ethnicGroupInd;
    @Column(name = "last_chg_time")
    private String lastChgTime;
    @Column(name = "marital_status_cd")
    private String maritalStatusCd;
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
    @Column(name = "dedup_match_ind")
    private String dedupMatchInd;
    @Column(name = "speaks_english_cd")
    private String speaksEnglishCd;
    @Column(name = "ethnic_unk_reason_cd")
    private String ethnicUnkReasonCd;
    @Column(name = "sex_unk_reason_cd")
    private String sexUnkReasonCd;
    @Column(name = "preferred_gender_cd")
    private String preferredGenderCd;
    @Column(name = "additional_gender_cd")
    private String additionalGenderCd;
    @Column(name = "occupation_cd")
    private String occupationCd;
    @Column(name = "prim_lang_cd")
    private String primLangCd;
    @Column(name = "add_user_id")
    private Long addUserId;
    @Column(name = "last_chg_user_id")
    private Long lastChgUserId;
    @Column(name = "multiple_birth_ind")
    private String multipleBirthInd;
    @Column(name = "adults_in_house_nbr")
    private String adultsInHouseNbr;
    @Column(name = "birth_order_nbr")
    private String birthOrderNbr;
    @Column(name = "children_in_house_nbr")
    private String childrenInHouseNbr;
    @Column(name = "education_level_cd")
    private String educationLevelCd;
    @Column(name = "PATIENT_NAME_NESTED")
    private String name;
    @Column(name = "PATIENT_ADDRESS_NESTED")
    private String address;
    @Column(name = "PATIENT_RACE_NESTED")
    private String race;
    @Column(name = "PATIENT_TELEPHONE_NESTED")
    private String telephone;
    @Column(name = "PATIENT_EMAIL_NESTED")
    private String email;
    @Column(name = "PATIENT_ENTITY_ID_NESTED")
    private String entityData;
    @Column(name = "PATIENT_ADD_AUTH_NESTED")
    private String addAuthNested;
    @Column(name = "PATIENT_CHG_AUTH_NESTED")
    private String chgAuthNested;

    /***
     * Transform the Name, Address, Race, Telephone, Email, EntityData(SSN), AddAuthUser, ChangeAuthUser
     * @return Fully Transformed Patient Object
     */
    public PatientFull processPatient() {
        PatientFull pf = new PatientFull().constructPersonFull(this);
        DataPostProcessor processor = new DataPostProcessor();
        try {
            processor.processPersonName(name, pf);
            processor.processPersonAddress(address, pf);
            processor.processPersonRace(race, pf);
            processor.processPersonTelephone(telephone, pf);
            processor.processPersonAddAuth(addAuthNested, pf);
            processor.processPersonChangeAuth(chgAuthNested, pf);
            processor.processPersonEntityData(entityData, pf);
            processor.processPersonEmail(email, pf);

        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
        }
        return pf;
    }
}
