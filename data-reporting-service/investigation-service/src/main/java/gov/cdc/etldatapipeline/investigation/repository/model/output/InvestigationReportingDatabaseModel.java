package gov.cdc.etldatapipeline.investigation.repository.model.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Set;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class InvestigationReportingDatabaseModel implements DataRequiredFields {
    private Long investigationUid;
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
    private Instant rptFormCmpltTime;
    private Instant activityToTime;
    private Instant activityFromTime;
    private Long addUserId;
    private String addUserName;
    private Instant addTime;
    private Long lastChgUserId;
    private String lastChgUserName;
    private Instant lastChgTime;
    private String currProcessStateCd;
    private String investigationStatusCd;
    private String recordStatusCd;
    private Long notificationLocalId;
    private Instant notificationAddTime;
    private String notificationRecordStatusCd;
    private Instant notificationLastChgTime;
    private Long investigatorId;
    private Long physicianId;
    private Long patientId;
    private Long organizationId;
    private String invStateCaseId;
    private String cityCountyCaseNbr;
    private String legacyCaseId;
    private Long phcInvFormId;

    @Override
    public Set<String> getRequiredFields() {
        return Set.of("investigationUid");
    }
}
