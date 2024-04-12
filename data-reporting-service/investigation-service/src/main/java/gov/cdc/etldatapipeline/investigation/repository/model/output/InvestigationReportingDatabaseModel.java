package gov.cdc.etldatapipeline.investigation.repository.model.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import lombok.NoArgsConstructor;

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
