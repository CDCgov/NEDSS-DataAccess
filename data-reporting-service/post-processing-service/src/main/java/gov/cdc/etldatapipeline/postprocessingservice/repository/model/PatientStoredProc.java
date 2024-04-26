package gov.cdc.etldatapipeline.postprocessingservice.repository.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;


/* THIS CLASS IS NOT BEING USED NOW BUT WE NEED THIS IN THE FUTURE
WHEN WE SEND DATA BACK FROM THE STORED PROC FOR FURTHER PROCESSING.
*/

@Entity
@Data
public class PatientStoredProc {

    @Column(name = "patient_key")
    private Long patientKey;

    @Column(name = "patient_mpr_uid")
    private Long patientMprUid;

    @Column(name = "patient_record_status")
    private String patientRecordStatus;

    @Column(name = "patient_local_id")
    private String patientLocalId;

    @Column(name = "patient_general_comments")
    private String patientGeneralComments;

    @Column(name = "patient_first_name")
    private String patientFirstName;

    @Column(name = "patient_middle_name")
    private String patientMiddleName;

    @Column(name = "patient_last_name")
    private String patientLastName;

    @Column(name = "patient_name_suffix")
    private String patientNameSuffix;

    @Column(name = "patient_alias_nickname")
    private String patientAliasNickname;

    @Column(name = "patient_street_address_1")
    private String patientStreetAddress1;

    @Column(name = "patient_street_address_2")
    private String patientStreetAddress2;

    @Column(name = "patient_city")
    private String patientCity;

    @Column(name = "patient_state")
    private String patientState;

    @Column(name = "patient_state_code")
    private String patientStateCode;

    @Column(name = "patient_zip")
    private String patientZip;

    @Column(name = "patient_county")
    private String patientCounty;

    @Column(name = "patient_county_code")
    private String patientCountyCode;

    @Column(name = "patient_country")
    private String patientCountry;

    @Column(name = "patient_within_city_limits")
    private String patientWithinCityLimits;

    @Column(name = "patient_phone_home")
    private String patientPhoneHome;

    @Column(name = "patient_phone_ext_home")
    private String patientPhoneExtHome;

    @Column(name = "patient_phone_work")
    private String patientPhoneWork;

    @Column(name = "patient_phone_ext_work")
    private String patientPhoneExtWork;

    @Column(name = "patient_phone_cell")
    private String patientPhoneCell;

    @Column(name = "patient_email")
    private String patientEmail;

    @Column(name = "patient_dob")
    private String patientDob;

    @Column(name = "patient_age_reported")
    private int patientAgeReported;

    @Column(name = "patient_age_reported_unit")
    private String patientAgeReportedUnit;

    @Column(name = "patient_birth_sex")
    private String patientBirthSex;

    @Column(name = "patient_current_sex")
    private String patientCurrentSex;

    @Column(name = "patient_deceased_indicator")
    private String patientDeceasedIndicator;

    @Column(name = "patient_deceased_date")
    private String patientDeceasedDate;

    @Column(name = "patient_marital_status")
    private String patientMaritalStatus;

    @Column(name = "patient_ssn")
    private String patientSsn;

    @Column(name = "patient_ethnicity")
    private String patientEthnicity;

    @Column(name = "patient_race_calculated")
    private String patientRaceCalculated;

    @Column(name = "patient_race_calc_details")
    private String patientRaceCalcDetails;

    @Column(name = "patient_race_amer_ind_1")
    private String patientRaceAmerInd1;

    @Column(name = "patient_race_amer_ind_2")
    private String patientRaceAmerInd2;

    @Column(name = "patient_race_amer_ind_3")
    private String patientRaceAmerInd3;

    @Column(name = "patient_race_amer_ind_gt3_ind")
    private String patientRaceAmerIndGt3Ind;

    @Column(name = "patient_race_amer_ind_all")
    private String patientRaceAmerIndAll;

    @Column(name = "patient_race_asian_1")
    private String patientRaceAsian1;

    @Column(name = "patient_race_asian_2")
    private String patientRaceAsian2;

    @Column(name = "patient_race_asian_3")
    private String patientRaceAsian3;

    @Column(name = "patient_race_asian_gt3_ind")
    private String patientRaceAsianGt3Ind;

    @Column(name = "patient_race_asian_all")
    private String patientRaceAsianAll;

    @Column(name = "patient_race_black_1")
    private String patientRaceBlack1;

    @Column(name = "patient_race_black_2")
    private String patientRaceBlack2;

    @Column(name = "patient_race_black_3")
    private String patientRaceBlack3;

    @Column(name = "patient_race_black_gt3_ind")
    private String patientRaceBlackGt3Ind;

    @Column(name = "patient_race_black_all")
    private String patientRaceBlackAll;

    @Column(name = "patient_race_nat_hi_1")
    private String patientRaceNatHi1;

    @Column(name = "patient_race_nat_hi_2")
    private String patientRaceNatHi2;

    @Column(name = "patient_race_nat_hi_3")
    private String patientRaceNatHi3;

    @Column(name = "patient_race_nat_hi_gt3_ind")
    private String patientRaceNatHiGt3Ind;

    @Column(name = "patient_race_nat_hi_all")
    private String patientRaceNatHiAll;

    @Column(name = "patient_race_white_1")
    private String patientRaceWhite1;

    @Column(name = "patient_race_white_2")
    private String patientRaceWhite2;

    @Column(name = "patient_race_white_3")
    private String patientRaceWhite3;

    @Column(name = "patient_race_white_gt3_ind")
    private String patientRaceWhiteGt3Ind;

    @Column(name = "patient_race_white_all")
    private String patientRaceWhiteAll;

    @Column(name = "patient_number")
    private String patientNumber;

    @Column(name = "patient_number_auth")
    private String patientNumberAuth;

    @Column(name = "patient_entry_method")
    private String patientEntryMethod;

    @Column(name = "patient_last_change_time")
    private String patientLastChangeTime;

    @Id
    @Column(name = "patient_uid")
    private Long patientUid;

    @Column(name = "patient_add_time")
    private String patientAddTime;

    @Column(name = "patient_added_by")
    private String patientAddedBy;

    @Column(name = "patient_last_updated_by")
    private String patientLastUpdatedBy;

    @Column(name = "patient_speaks_english")
    private String patientSpeaksEnglish;

    @Column(name = "patient_unk_ethnic_rsn")
    private String patientUnkEthnicRsn;

    @Column(name = "patient_curr_sex_unk_rsn")
    private String patientCurrSexUnkRsn;

    @Column(name = "patient_preferred_gender")
    private String patientPreferredGender;

    @Column(name = "patient_addl_gender_info")
    private String patientAddlGenderInfo;

    @Column(name = "patient_census_tract")
    private String patientCensusTract;

    @Column(name = "patient_race_all")
    private String patientRaceAll;

    @Column(name = "patient_birth_country")
    private String patientBirthCountry;

    @Column(name = "patient_primary_occupation")
    private String patientPrimaryOccupation;

    @Column(name = "patient_primary_language")
    private String patientPrimaryLanguage;
}
