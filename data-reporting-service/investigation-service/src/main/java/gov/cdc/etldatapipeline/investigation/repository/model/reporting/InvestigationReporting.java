package gov.cdc.etldatapipeline.investigation.repository.model.reporting;

import lombok.Data;

@Data
public class InvestigationReporting {
    private Long publicHealthCaseUid;
    private Long programJurisdictionOid;
    private String jurisdictionCode;
    private String jurisdictionNm;
    private String moodCd;
    private String classCd;
    private String caseTypeCd;
    private String caseClassCd;
    private String invCaseStatus;
    private String outbreakName;
    private String cd;
    private String cdDescTxt;
    private String progAreaCd;
    private String jurisdictionCd;
    private String pregnantIndCd;
    private String pregnantInd;
    private String localId;
    private String rptFormCmpltTime;
    private String activityToTime;
    private String activityFromTime;
    private String currProcessStateCd;
    private String currProcessState;
    private String investigationStatusCd;
    private String investigationStatus;
    private String recordStatusCd;
    private String sharedInd;
    private String txt;
    private String effectiveFromTime;
    private String effectiveToTime;
    private String rptSourceCd;
    private String rptSrcCdDesc;
    private String rptToCountyTime;
    private String rptToStateTime;
    private String mmwrWeek;
    private String mmwrYear;
    private String diseaseImportedCd;
    private String diseaseImportedInd;
    private String importedCountryCd;
    private String importedStateCd;
    private String importedCountyCd;
    private String importedFromCountry;
    private String importedFromState;
    private String importedFromCounty;
    private String importedCityDescTxt;
    private String diagnosisTime;
    private String hospitalizedAdminTime;
    private String hospitalizedDischargeTime;
    private Long hospitalizedDurationAmt;
    private String outbreakInd;
    private String outbreakIndVal;
    private String hospitalizedIndCd;
    private String hospitalizedInd;
    private String transmissionModeCd;
    private String transmissionMode;
    private String outcomeCd;
    private String dieFrmThisIllnessInd;
    private String dayCareIndCd;
    private String dayCareInd;
    private String foodHandlerIndCd;
    private String foodHandlerInd;
    private String deceasedTime;
    private String patAgeAtOnset;
    private String patAgeAtOnsetUnitCd;
    private String patAgeAtOnsetUnit;
    private String detectionMethodDescTxt;
    private String contactInvPriority;
    private String contactInvStatus;
    private String investigatorAssignedTime;
    private String effectiveDurationAmt;
    private String effectiveDurationUnitCd;
    private String illnessDurationUnit;
    private String infectiousFromDate;
    private String infectiousToDate;
    private String referralBasisCd;
    private String referralBasis;
    private String invPriorityCd;
    private String coinfectionId;
    private String contactInvTxt;
    private String programAreaDescription;
    private String notificationLocalId;
    private String notificationAddTime;
    private String notificationRecordStatusCd;
    private String notificationLastChgTime;
    private Long investigatorId;
    private Long physicianId;
    private Long patientId;
    private Long organizationId;
    private String invStateCaseId;
    private String cityCountyCaseNbr;
    private String legacyCaseId;
    private Long phcInvFormId;
    private Long addUserId;
    private String addUserName;
    private String addTime;
    private Long lastChgUserId;
    private String lastChgUserName;
    private String lastChgTime;
}
