package gov.cdc.datareportingservice.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.datareportingservice.changedata.model.odse.DebeziumMetadata;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
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
    @Column(name = "PATIENT_NAME")
    private String patientName;
    @Column(name = "PATIENT_ADDRESS")
    private String patientAddress;
    @Column(name = "PATIENT_TELEPHONE")
    private String patientTelephone;
    @Column(name = "PATIENT_EMAIL")
    private String patientEmail;
    @Column(name = "PATIENT_RACE")
    private String patientRace;
    @Column(name = "PATIENT_ENTITY_ID")
    private String patientEntityId;
    @Column(name = "PATIENT_AUTH")
    private String patientAuth;
    @Column(name = "PATIENT_BIRTH_COUNTRY")
    private String patientBirthCountry;
}


/*
package gov.cdc.datareportingservice.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.datareportingservice.changedata.model.odse.DebeziumMetadata;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatientOP extends DebeziumMetadata {
    @Id
    @Column(name = "PATIENT_UID")
    private String patientUid;
    @Column(name = "PATIENT_LOCAL_ID")
    private String patientLocalId;
    @Column(name = "PATIENT_GENERAL_COMMENTS")
    private String patientGeneralComments;
    @Column(name = "PATIENT_ENTRY_METHOD")
    private String patientEntryMethod;
    @Column(name = "PATIENT_LAST_CHANGE_TIME")
    private String patientLastChangeTime;
    @Column(name = "PATIENT_ADD_TIME")
    private String patientAddTime;
    @Column(name = "PATIENT_RECORD_STATUS")
    private String patientRecordStatus;
    @Column(name = "PATIENT_FIRST_NAME")
    private String patientFirstName;
    @Column(name = "PATIENT_MIDDLE_NAME")
    private String patientMiddleName;
    @Column(name = "PATIENT_LAST_NAME")
    private String patientLastName;
    @Column(name = "PATIENT_NAME_SUFFIX")
    private String patientNameSuffix;
    @Column(name = "PATIENT_STREET_ADDRESS_1")
    private String patientStreetAddress1;
    @Column(name = "PATIENT_STREET_ADDRESS_2")
    private String patientStreetAddress2;
    @Column(name = "PATIENT_CITY")
    private String patientCity;
    @Column(name = "PATIENT_STATE")
    private String patientState;
    @Column(name = "PATIENT_STATE_CODE")
    private String patientStateCode;
    @Column(name = "PATIENT_ZIP")
    private String patientZip;
    @Column(name = "PATIENT_COUNTY")
    private String patientCounty;
    @Column(name = "PATIENT_COUNTY_CODE")
    private String patientCountyCode;
    @Column(name = "PATIENT_COUNTRY")
    private String patientCountry;
    @Column(name = "PATIENT_PHONE_WORK")
    private String patientPhoneWork;
    @Column(name = "PATIENT_PHONE_EXT_WORK")
    private String patientPhoneExtWork;
    @Column(name = "PATIENT_EMAIL")
    private String patientEmail;
    @Column(name = "PATIENT_PHONE_CELL")
    private String patientPhoneCell;
    @Column(name = "PATIENT_ADDED_BY")
    private String patientAddedBy;
    @Column(name = "PATIENT_LAST_UPDATED_BY")
    private String patientLastUpdatedBy;
}

 */