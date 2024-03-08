package gov.cdc.etldatapipeline.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.changedata.model.dto.patient.*;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatientOP extends DebeziumMetadata {
    @Id
    @Column(name = "PATIENT_UID")
    private String patientUid;
    @Column(name = "PATIENT_MPR_UID")
    private String patientMprUid;
    @Column(name = "PATIENT_GENERAL_COMMENTS")
    private String patientGeneralComments;
    @Column(name = "PATIENT_ADD_TIME")
    private String patientAddTime;
    @Column(name = "PATIENT_RECORD_STATUS")
    private String patientRecordStatus;
    @Column(name = "PATIENT_LOCAL_ID")
    private String patientLocalId;
    @Column(name = "PATIENT_AGE_REPORTED")
    private String patientAgeReported;
    @Column(name = "PATIENT_AGE_REPORTED_UNIT")
    private String patientAgeReportedUnit;
    @Column(name = "PATIENT_CURRENT_SEX")
    private String patientCurrentSex;
    @Column(name = "PATIENT_ENTRY_METHOD")
    private String patientEntryMethod;
    @Column(name = "PATIENT_LAST_CHANGE_TIME")
    private String patientLastChangeTime;
    @Column(name = "PATIENT_FIRST_NAME")
    private String patientFirstName;
    @Column(name = "PATIENT_MIDDLE_NAME")
    private String patientMiddleName;
    @Column(name = "PATIENT_LAST_NAME")
    private String patientLastName;
    @Column(name = "PATIENT_NAME_SUFFIX")
    private String patientNameSuffix;
    @Column(name = "PATIENT_NAME_NESTED")
    private String patientName;
    @Column(name = "PATIENT_ADDRESS_NESTED")
    private String patientAddress;
    @Column(name = "PATIENT_TELEPHONE_NESTED")
    private String patientTelephone;
    @Column(name = "PATIENT_EMAIL")
    private String patientEmail;
    @Column(name = "PATIENT_RACE")
    private String patientRace;
    private String patientRaceCd;
    private String patientRaceDesc;
    private String patientRaceCategory;
    @Column(name = "PATIENT_ENTITY_ID_NESTED")
    private String patientEntityData;
    @Column(name = "PATIENT_AUTH")
    private String patientAuth;
    @Column(name = "PATIENT_BIRTH_COUNTRY")
    private String patientBirthCountry;
    @Column(name = "PATIENT_ADD_AUTH_NESTED")
    private String patientAddAuthNested;
    @Column(name = "PATIENT_CHG_AUTH_NESTED")
    private String patientChgAuthNested;
    private String patientStreetAddress1;
    private String patientStreetAddress2;
    private String patientCity;
    private String patientState;
    private String patientStateCode;
    private String patientZip;
    private String patientCounty;
    private String patientCountyCode;
    private String patientCountry;
    private String patientCountryCode;
    private String patientPhoneWork;
    private String patientPhoneExtWork;
    private String patientPhoneHome;
    private String patientPhoneExtHome;
    private String patientPhoneCell;
    private String patientSsn;
    private Long patientAddedBy;
    private Long patientLastChangedBy;

    public PatientOP processPatient() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            if (!ObjectUtils.isEmpty(patientName)) {
                Arrays.stream(mapper.readValue(patientName, Name[].class))
                        .max(Comparator.comparing(Name::getPersonUid))
                        .map(n -> n.updatePerson(this));
            }
            if (!ObjectUtils.isEmpty(patientAddress)) {
                Arrays.stream(mapper.readValue(patientAddress, Address[].class))
                        .max(Comparator.comparing(Address::getPostalLocatorUid))
                        .map(n -> n.updatePerson(this));
            }
            if (!ObjectUtils.isEmpty(patientRace)) {
                Arrays.stream(mapper.readValue(patientRace, Race[].class))
                        .max(Comparator.comparing(Race::getPersonUid))
                        .map(n -> n.updatePerson(this));
            }

            if (!ObjectUtils.isEmpty(patientTelephone)) {
                Function<String, PatientOP> patientPhoneFn =
                        (useCd) -> Arrays.stream(UtilHelper.getInstance().deserializePayload(patientTelephone,
                                        Phone[].class))
                                .filter(phone -> phone.getUseCd().equalsIgnoreCase(useCd))
                                .max(Comparator.comparing(Phone::getTeleLocatorUid))
                                .map(n -> n.updatePerson(this))
                                .orElse(null);
                patientPhoneFn.apply("WP");
                patientPhoneFn.apply("H");
                patientPhoneFn.apply("C");
            }

            if (!ObjectUtils.isEmpty(patientAddAuthNested)) {
                Arrays.stream(mapper.readValue(patientAddAuthNested, AddAuthUser[].class))
                        .max(Comparator.comparing(AddAuthUser::getAddUserChgTime))
                        .map(n -> n.updatePerson(this));
            }

            if (!ObjectUtils.isEmpty(patientChgAuthNested)) {
                Arrays.stream(mapper.readValue(patientChgAuthNested, ChgAuthUser[].class))
                        .max(Comparator.comparing(ChgAuthUser::getLastChgUserTime))
                        .map(n -> n.updatePerson(this));
            }

            if (!ObjectUtils.isEmpty(patientEntityData)) {
                Arrays.stream(mapper.readValue(patientEntityData, EntityData[].class))
                        .filter(e -> e.getAssigningAuthorityCd().equalsIgnoreCase("SSA"))
                        .max(Comparator.comparing(EntityData::getEntityIdSeq))
                        .map(n -> n.updatePerson(this));
            }

        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return this;
    }
}
