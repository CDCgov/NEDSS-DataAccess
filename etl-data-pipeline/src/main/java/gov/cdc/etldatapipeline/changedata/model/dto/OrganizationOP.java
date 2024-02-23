package gov.cdc.etldatapipeline.changedata.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
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
public class OrganizationOP extends DebeziumMetadata {
    @Id
    @Column(name = "ORGANIZATION_UID")
    private String organizationUid;
    @Column(name = "ORGANIZATION_LOCAL_ID")
    private String organizationLocalId;
    @Column(name = "ORGANIZATION_RECORD_STATUS")
    private String organizationRecordStatus;
    @Column(name = "ORGANIZATION_NAME")
    private String organizationName;
    @Column(name = "ORGANIZATION_GENERAL_COMMENTS")
    private String organizationGeneralComments;
    @Column(name = "ORGANIZATION_QUICK_CODE")
    private String organizationQuickCode;
    @Column(name = "ORGANIZATION_STAND_IND_CLASS")
    private String organizationStandIndClass;
    @Column(name = "ORGANIZATION_FACILITY_ID")
    private String organizationFacilityId;
    @Column(name = "ORGANIZATION_FACILITY_ID_AUTH")
    private String organizationFacilityIdAuth;
    @Column(name = "ORGANIZATION_STREET_ADDRESS_1")
    private String organizationStreetAddress1;
    @Column(name = "ORGANIZATION_STREET_ADDRESS_2")
    private String organizationStreetAddress2;
    @Column(name = "ORGANIZATION_CITY")
    private String organizationCity;
    @Column(name = "ORGANIZATION_STATE")
    private String organizationState;
    @Column(name = "ORGANIZATION_STATE_CODE")
    private String organizationStateCode;
    @Column(name = "ORGANIZATION_ZIP")
    private String organizationZip;
    @Column(name = "ORGANIZATION_COUNTY")
    private String organizationCounty;
    @Column(name = "ORGANIZATION_COUNTY_CODE")
    private String organizationCountyCode;
    @Column(name = "ORGANIZATION_COUNTRY")
    private String organizationCountry;
    @Column(name = "ORGANIZATION_ADDRESS_COMMENTS")
    private String organizationAddressComments;
    @Column(name = "ORGANIZATION_PHONE_WORK")
    private String organizationPhoneWork;
    @Column(name = "ORGANIZATION_PHONE_EXT_WORK")
    private String organizationPhoneExtWork;
    @Column(name = "ORGANIZATION_EMAIL")
    private String organizationEmail;
    @Column(name = "ORGANIZATION_PHONE_COMMENTS")
    private String organizationPhoneComments;
    @Column(name = "ORGANIZATION_PHONE_CELL")
    private String organizationPhoneCell;
    @Column(name = "ORGANIZATION_LAST_CHANGE_TIME")
    private String organizationLastChangeTime;
    @Column(name = "ORGANIZATION_ADD_TIME")
    private String organizationAddTime;
    @Column(name = "ORGANIZATION_ADDED_BY")
    private String organizationAddedBy;
    @Column(name = "ORGANIZATION_LAST_UPDATED_BY")
    private String organizationLastUpdatedBy;
    @Column(name = "ORGANIZATION_FAX")
    private String organizationFax;

}

