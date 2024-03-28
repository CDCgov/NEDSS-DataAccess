package gov.cdc.etldatapipeline.person.model.dto.patient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import gov.cdc.etldatapipeline.person.utils.DataPostProcessor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Entity
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Patient {
    @Id
    @Column(name = "person_uid")
    private Long patientUid;
    @Column(name = "age_reported")
    private String ageReported;
    @Column(name = "age_reported_unit_cd")
    private String ageReportedUnitCd;
    @Column(name = "additional_gender_cd")
    private String additionalGenderCd;
    @Column(name = "add_user_id")
    private Long addUserId;
    @Column(name = "add_user_name")
    private String addUserName;
    @Column(name = "add_time")
    private String addTime;
    @Column(name = "adults_in_house_nbr")
    private String adultsInHouseNbr;
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
    @Column(name = "birth_gender_cd")
    private String birthGenderCd;
    @Column(name = "birth_order_nbr")
    private String birthOrderNbr;
    @Column(name = "birth_time")
    private String birthTime;
    @Column(name = "cd")
    private String cd;
    @Column(name = "children_in_house_nbr")
    private String childrenInHouseNbr;
    @Column(name = "curr_sex_cd")
    private String currSexCd;
    @Column(name = "deceased_ind_cd")
    private String deceasedIndCd;
    @Column(name = "deceased_time")
    private String deceasedTime;
    @Column(name = "dedup_match_ind")
    private String dedupMatchInd;
    @Column(name = "description")
    private String description;
    @Column(name = "education_level_cd")
    private String educationLevelCd;
    @Column(name = "edx_ind")
    private String edxInd;
    @Column(name = "electronic_ind")
    private String electronicInd;
    @Column(name = "ethnic_group_ind")
    private String ethnicGroupInd;
    @Column(name = "ethnic_unk_reason_cd")
    private String ethnicUnkReasonCd;
    @Column(name = "last_chg_time")
    private String lastChgTime;
    @Column(name = "last_chg_user_id")
    private Long lastChgUserId;
    @Column(name = "last_chg_user_name")
    private String lastChgUserName;
    @Column(name = "local_id")
    private String localId;
    @Column(name = "marital_status_cd")
    private String maritalStatusCd;
    @Column(name = "multiple_birth_ind")
    private String multipleBirthInd;
    @Column(name = "occupation_cd")
    private String occupationCd;
    @Column(name = "person_parent_uid")
    private Long personParentUid;
    @Column(name = "first_nm")
    private String personFirstNm;
    @Column(name = "last_nm")
    private String personLastNm;
    @Column(name = "middle_nm")
    private String personMiddleNm;
    @Column(name = "nm_suffix")
    private String personNmSuffix;
    @Column(name = "preferred_gender_cd")
    private String preferredGenderCd;
    @Column(name = "prim_lang_cd")
    private String primLangCd;
    @Column(name = "record_status_cd")
    private String recordStatusCd;
    @Column(name = "record_status_time")
    private String recordStatusTime;
    @Column(name = "status_cd")
    private String statusCd;
    @Column(name = "speaks_english_cd")
    private String speaksEnglishCd;
    @Column(name = "status_time")
    private String statusTime;
    @Column(name = "sex_unk_reason_cd")
    private String sexUnkReasonCd;
    @Column(name = "version_ctrl_nbr")
    private String versionCtrlNbr;

    //Following are Json data formatted nested fields
    @Column(name = "patient_name")
    private String nameNested;
    @Column(name = "patient_address")
    private String addressNested;
    @Column(name = "patient_race")
    private String raceNested;
    @Column(name = "patient_telephone")
    private String telephoneNested;
    @Column(name = "patient_email")
    private String emailNested;
    @Column(name = "patient_entity")
    private String entityDataNested;

    /***
     * Transform the Name, Address, Race, Telephone, Email, EntityData(SSN), AddAuthUser, ChangeAuthUser
     * @return Fully Transformed Patient Object
     */
    public PatientReporting processPatientReporting() {
        PatientReporting pf = new PatientReporting().constructPatientReporting(this);
        DataPostProcessor processor = new DataPostProcessor();
        try {
            processor.processPersonName(nameNested, pf);
            processor.processPersonAddress(addressNested, pf);
            processor.processPersonRace(raceNested, pf);
            processor.processPersonTelephone(telephoneNested, pf);
            processor.processPersonEntityData(entityDataNested, pf);
            processor.processPersonEmail(emailNested, pf);

        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
        }
        return pf;
    }

    public PatientElasticSearch processPatientElastic() {
        PatientElasticSearch pf = new PatientElasticSearch().constructObject(this);
        DataPostProcessor processor = new DataPostProcessor();
        try {
            processor.processPersonName(nameNested, pf);
            processor.processPersonAddress(addressNested, pf);
            processor.processPersonRace(raceNested, pf);
            processor.processPersonTelephone(telephoneNested, pf);
            processor.processPersonEntityData(entityDataNested, pf);
            processor.processPersonEmail(emailNested, pf);

        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
        }
        return pf;
    }
}
