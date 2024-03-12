package gov.cdc.etldatapipeline.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
import jakarta.persistence.*;
import lombok.*;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode(callSuper=true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Provider extends DebeziumMetadata {
    @Id
    @Column(name = "PROVIDER_UID")
    private String providerUid;
    @Column(name = "PROVIDER_LOCAL_ID")
    private String providerLocalId;
    @Column(name = "PROVIDER_GENERAL_COMMENTS")
    private String providerGeneralComments;
    @Column(name = "PROVIDER_ENTRY_METHOD")
    private String providerEntryMethod;
    @Column(name = "PROVIDER_LAST_CHANGE_TIME")
    private String providerLastChangeTime;
    @Column(name = "PROVIDER_ADD_TIME")
    private String providerAddTime;
    @Column(name = "PROVIDER_RECORD_STATUS")
    private String providerRecordStatus;
    @Column(name = "PROVIDER_NAME_PREFIX")
    private String providerNamePrefix;
    @Column(name = "PROVIDER_FIRST_NAME")
    private String providerFirstName;
    @Column(name = "PROVIDER_MIDDLE_NAME")
    private String providerMiddleName;
    @Column(name = "PROVIDER_LAST_NAME")
    private String providerLastName;
    @Column(name = "PROVIDER_NAME_SUFFIX")
    private String providerNameSuffix;
    @Column(name = "PROVIDER_NAME_DEGREE")
    private String providerNameDegree;
    @Column(name = "PROVIDER_QUICK_CODE")
    private String providerQuickCode;
    @Column(name = "PROVIDER_REGISTRATION_NUM")
    private String providerRegistrationNum;
    @Column(name = "PROVIDER_REGISRATION_NUM_AUTH")
    private String providerRegisrationNumAuth;
    @Column(name = "PROVIDER_STREET_ADDRESS_1")
    private String providerStreetAddress1;
    @Column(name = "PROVIDER_STREET_ADDRESS_2")
    private String providerStreetAddress2;
    @Column(name = "PROVIDER_CITY")
    private String providerCity;
    @Column(name = "PROVIDER_STATE")
    private String providerState;
    @Column(name = "PROVIDER_STATE_CODE")
    private String providerStateCode;
    @Column(name = "PROVIDER_ZIP")
    private String providerZip;
    @Column(name = "PROVIDER_COUNTY")
    private String providerCounty;
    @Column(name = "PROVIDER_COUNTY_CODE")
    private String providerCountyCode;
    @Column(name = "PROVIDER_COUNTRY")
    private String providerCountry;
    @Column(name = "PROVIDER_ADDRESS_COMMENTS")
    private String providerAddressComments;
    @Column(name = "PROVIDER_PHONE_WORK")
    private String providerPhoneWork;
    @Column(name = "PROVIDER_PHONE_EXT_WORK")
    private String providerPhoneExtWork;
    @Column(name = "PROVIDER_EMAIL_WORK")
    private String providerEmailWork;
    @Column(name = "PROVIDER_PHONE_COMMENTS")
    private String providerPhoneComments;
    @Column(name = "PROVIDER_PHONE_CELL")
    private String providerPhoneCell;
    @Column(name = "provider_added_by")
    private String providerAddedBy;
    @Column(name = "provider_last_updated_by")
    private String providerLastUpdatedBy;
}

