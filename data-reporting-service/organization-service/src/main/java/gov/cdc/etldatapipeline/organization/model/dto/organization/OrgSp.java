package gov.cdc.etldatapipeline.organization.model.dto.organization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import gov.cdc.etldatapipeline.organization.utils.DataPostProcessor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrgSp {
    @Id
    @Column(name = "organization_uid")
    private Long organizationUid;
    @Column(name = "organization_local_id")
    private String organizationLocalId;
    @Column(name = "organization_record_status")
    private String organizationRecordStatus;
    @Column(name = "organization_name")
    private String organizationName;
    @Column(name = "organization_general_comments")
    private String organizationGeneralComments;
    @Column(name = "organization_quick_code")
    private String organizationQuickCode;
    @Column(name = "organization_stand_ind_class")
    private String organizationStandIndClass;
    @Column(name = "organization_facility_id")
    private String organizationFacilityId;
    @Column(name = "organization_facility_id_auth")
    private String organizationFacilityIdAuth;
    @Column(name = "organization_street_address1")
    private String organizationStreetAddress1;
    @Column(name = "organization_street_address2")
    private String organizationStreetAddress2;
    @Column(name = "organization_city")
    private String organizationCity;
    @Column(name = "organization_state")
    private String organizationState;
    @Column(name = "organization_state_code")
    private String organizationStateCode;
    @Column(name = "organization_zip")
    private String organizationZip;
    @Column(name = "organization_county")
    private String organizationCounty;
    @Column(name = "organization_county_code")
    private String organizationCountyCode;
    @Column(name = "organization_country")
    private String organizationCountry;
    @Column(name = "organization_address_comments")
    private String organizationAddressComments;
    @Column(name = "organization_phone_work")
    private String organizationPhoneWork;
    @Column(name = "organization_phone_ext_work")
    private String organizationPhoneExtWork;
    @Column(name = "organization_email")
    private String organizationEmail;
    @Column(name = "organization_phone_comments")
    private String organizationPhoneComments;
    @Column(name = "organization_last_change_time")
    private String organizationLastChangeTime;
    @Column(name = "organization_add_time")
    private String organizationAddTime;
    @Column(name = "organization_added_by")
    private String organizationAddedBy;
    @Column(name = "organization_last_updated_by")
    private String organizationLastUpdatedBy;
    @Column(name = "organization_fax")
    private String organizationFax;

    public OrgReporting processOrgReporting() {
        return postProcessJsonData(new OrgReporting().constructObject(this));
    }

    public OrgReporting processOrgElastic() {
        return postProcessJsonData(new OrgReporting().constructObject(this));
    }

    private <T> T postProcessJsonData(T pf) {
        DataPostProcessor processor = new DataPostProcessor();
        return pf;
    }
}

