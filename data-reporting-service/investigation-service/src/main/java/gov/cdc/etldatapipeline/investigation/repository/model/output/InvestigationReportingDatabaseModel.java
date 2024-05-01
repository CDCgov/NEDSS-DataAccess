package gov.cdc.etldatapipeline.investigation.repository.model.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.persistence.Column;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Set;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class InvestigationReportingDatabaseModel implements DataRequiredFields {
    private Long publicHealthCaseUid;
    private Long programJurisdictionOid;
    private String jurisdictionCode;
    private String jurisdictionCodeDescTxt;
    private String moodCd;
    private String classCd;
    private String caseTypeCd;
    private String caseClassCd;
    private String outbreakName;
    private String cd;
    private String cdDescTxt;
    private String progAreaCd;
    private String jurisdictionCd;
    private String pregnantIndCd;
    private String localId;
    private String rptFormCmpltTime;
    private String activityToTime;
    private String activityFromTime;
    private String currProcessStateCd;
    private String investigationStatusCd;
    private String recordStatusCd;
    private String sharedInd;
    private String txt;
    private Instant effectiveFromTime;
    private Instant effectiveToTime;
    private String rptSourceCd;
    private String rptSourceCdDesc;

    
    private Instant rptToCountyTime;

    
    private Instant rptToStateTime;

    
    private String mmwrWeek;

    
    private String mmwrYear;

    
    private String diseaseImportedCd;

    
    private String diseaseImportedInd;

    
    private String importedCountryCd;

    
    private String importedStateCd;

    
    private String importedCountyCd;

    //
    //private String importFrmCityCd;

    
    private Instant diagnosisTime;

    
    private Instant hospitalizedAdminTime;

    
    private Instant hospitalizedDischargeTime;

    
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

    
    private Instant deceasedTime;

    
    private String patAgeAtOnsetUnitCd;

    
    private String patAgeAtOnsetUnit;

    
    private Instant investigatorAssignedTime;

    
    private String effectiveDurationAmt;

    
    private String effectiveDurationUnitCd;

    
    private String illnessDurationUnit;

    
    private Instant infectiousFromDate;

    
    private Instant infectiousToDate;

    
    private String referralBasisCd;

    
    private String referralBasis;

    
    private String invPriorityCd;

    
    private String coinfectionId;

    //
    //private String outbreakNameDesc;

    
    private String programAreaDescription;
    private String notificationLocalId;
    private String notificationAddTime;
    private String notificationRecordStatusCd;
    private String notificationLastChgTime;
    private Long investigatorId;
    private Long physicianId;
    private Long patientId;
    private Long organizationId;
    private Long invStateCaseId;
    private Long cityCountyCaseNbr;
    private Long legacyCaseId;
    private Long phcInvFormId;
    private Long addUserId;
    private String addUserName;
    private String addTime;
    private Long lastChgUserId;
    private String lastChgUserName;
    private String lastChgTime;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("publicHealthCaseUid");
    }
}
