package gov.cdc.etldatapipeline.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.changedata.model.dto.persondetail.*;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;

@Slf4j
@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper=true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PersonOp extends DebeziumMetadata {
    @Id
    @Column(name = "person_uid")
    private String personUid;
    @Column(name = "person_parent_uid")
    private String personParentUid;
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
    private String addUserId;
    @Column(name = "last_chg_user_id")
    private String lastChgUserId;
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
    @Column(name = "PERSON_NAME_NESTED")
    private String name;
    @Column(name = "PERSON_ADDRESS_NESTED")
    private String address;
    @Column(name = "PERSON_RACE_NESTED")
    private String race;
    @Column(name = "PERSON_TELEPHONE_NESTED")
    private String telephone;
    @Column(name = "PERSON_EMAIL_NESTED")
    private String email;
    @Column(name = "PERSON_ENTITY_ID_NESTED")
    private String entityData;
    @Column(name = "PERSON_ADD_AUTH_NESTED")
    private String addAuthNested;
    @Column(name = "PERSON_CHG_AUTH_NESTED")
    private String chgAuthNested;

    public PersonFull processPatient() {
        PersonFull personFull = new PersonFull().constructPersonFull(this);
        try {
            ObjectMapper mapper = new ObjectMapper();
            if (!ObjectUtils.isEmpty(name)) {
                Arrays.stream(mapper.readValue(name, Name[].class))
                        .max(Comparator.comparing(Name::getPersonUid))
                        .map(n -> n.updatePerson(personFull));
            }
            if (!ObjectUtils.isEmpty(address)) {
                Arrays.stream(mapper.readValue(address, Address[].class))
                        .max(Comparator.comparing(Address::getPostalLocatorUid))
                        .map(n -> n.updatePerson(personFull));
            }
            if (!ObjectUtils.isEmpty(race)) {
                Arrays.stream(mapper.readValue(race, Race[].class))
                        .max(Comparator.comparing(Race::getPersonUid))
                        .map(n -> n.updatePerson(personFull));
            }

            if (!ObjectUtils.isEmpty(telephone)) {
                Function<String, PersonOp> patientPhoneFn =
                        (useCd) -> Arrays.stream(
                                UtilHelper.getInstance().deserializePayload(telephone, Phone[].class))
                                .filter(phone -> phone.getUseCd().equalsIgnoreCase(useCd))
                                .max(Comparator.comparing(Phone::getTeleLocatorUid))
                                .map(n -> n.updatePerson(personFull))
                                .orElse(null);
                patientPhoneFn.apply("WP");
                patientPhoneFn.apply("H");
                patientPhoneFn.apply("C");
            }

            if (!ObjectUtils.isEmpty(addAuthNested)) {
                Arrays.stream(mapper.readValue(addAuthNested, AddAuthUser[].class))
                        .max(Comparator.comparing(AddAuthUser::getAddUserChgTime))
                        .map(n -> n.updatePerson(personFull));
            }

            if (!ObjectUtils.isEmpty(chgAuthNested)) {
                Arrays.stream(mapper.readValue(chgAuthNested, ChgAuthUser[].class))
                        .max(Comparator.comparing(ChgAuthUser::getLastChgUserTime))
                        .map(n -> n.updatePerson(personFull));
            }

            if (!ObjectUtils.isEmpty(entityData)) {
                Arrays.stream(mapper.readValue(entityData, EntityData[].class))
                        .filter(e -> e.getAssigningAuthorityCd().equalsIgnoreCase("SSA"))
                        .max(Comparator.comparing(EntityData::getEntityIdSeq))
                        .map(n -> n.updatePerson(personFull));
            }

            if (!ObjectUtils.isEmpty(email)) {
                Arrays.stream(mapper.readValue(email, Email[].class))
                        .max(Comparator.comparing(Email::getTeleLocatorUid))
                        .map(n -> n.updatePerson(personFull));
            }

        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return personFull;
    }
}
